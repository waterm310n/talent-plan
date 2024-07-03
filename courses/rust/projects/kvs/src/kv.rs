// kvstore

use serde_json::{Deserializer as JsonDeserializer,self};
use std::collections::HashMap;
use std::fs;
use std::fs::{File,OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::command::CommandPos;
use crate::error::Result;
use crate::DataCommand;
use crate::KvsError;

const MAX_USELESS_SIZE: u64 = 0x10_000; //最大无用大小

/// 带有当前写入位置终点的BufWriter
pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    file_id: u64,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W,file_id:u64) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?; //获取当前文件的尾部位置,因为总是从尾部位置开始Append
        Ok(Self {
            writer: BufWriter::new(inner),
            file_id,
            pos,
        })
    }

    // 像日志中写入一条命令，同时返回写入的位置与写入的命令长度
    pub fn write_command(&mut self, command: DataCommand) -> Result<(u64, u64)> {
        let result = serde_json::to_vec(&command)?;
        let prev_pos = self.pos;
        self.writer.write_all(result.as_slice())?;
        let length = self
            .writer
            .seek(SeekFrom::Current(0))
            .map(|cur_pos| {
                self.pos = cur_pos; //更新当前位置
                cur_pos - prev_pos
            })?;
        
        Ok((prev_pos, length))
    }
}

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    key_dir: HashMap<String, CommandPos>,     // 内存中的哈希表
    readers: HashMap<u64, BufReader<File>>, // 缓存所有已经关闭的文件，适用于频繁小数据读
    writer: BufWriterWithPos<File>,           // 适用于频繁小数据写
    data_dir: PathBuf,                        // 数据目录
    useless_size: u64,                        // 当前无用的数据总和，超过一定值则进行文件Merge
}

impl KvStore {
    fn get_writer(
        mut path_buf: PathBuf,
        log_file_list: &mut Vec<u64>,
    ) -> Result<BufWriterWithPos<File>> {
        log_file_list.push(log_file_list.len() as u64);
        path_buf.push(format!("{}.log", log_file_list[log_file_list.len() - 1]));
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path_buf)?;
        return Ok(BufWriterWithPos::new(file,log_file_list[log_file_list.len() - 1])?);
    }

    /// 返回数据目录中的数据文件，并返回排序的数据文件名，此处的文件名默认为数字
    fn sorted_log_list(data_dir: &PathBuf) -> Vec<u64> {
        if let Ok(entries) = fs::read_dir(data_dir) {
            let mut log_file_list = vec![];
            for entry in entries {
                if let Ok(entry) = entry {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.ends_with(".log") {
                            if let Ok(file_id) = file_name[..file_name.len() - 4].parse::<u64>() {
                                log_file_list.push(file_id);
                            }
                        }
                    }
                }
            }
            log_file_list.sort_unstable();
            return log_file_list;
        }
        return vec![];
    }

    // 读取日志文件，并修改kvstore状态，同时返回其中无用字节数量
    fn read_log_files(
        key_dir: &mut HashMap<String, CommandPos>,
        readers: &mut HashMap<u64, BufReader<File>>,
        mut path_buf: PathBuf,
        file_id: u64,
    ) -> Result<u64> {
        path_buf.push(format!("{}.log", file_id));
        let file = OpenOptions::new().read(true).open(path_buf)?;
        let mut buf_reader = BufReader::new(file);
        let mut cur_pos = buf_reader.seek(SeekFrom::Start(0))?;
        // 使用serdejson的反序列化器将数据转换为json序列流
        let mut stream =  JsonDeserializer::from_reader(&mut buf_reader).into_iter::<DataCommand>();
        let mut useless_size: u64 = 0;
        // 使用while let而不是for循环，因为for循环无法获取长度与位置
        while let Some(cmd) = stream.next(){
            let next_pos = stream.byte_offset() as u64; // 获取读取一条命令后的位置
            let cmd_len = next_pos - cur_pos;
            match cmd? {
                DataCommand::Set { key, value:_ } => {
                    key_dir.entry(key)
                        .and_modify(|x| useless_size += x.change(file_id, cmd_len,cur_pos))
                        .or_insert(CommandPos{file_id:file_id,value_size:cmd_len,value_pos:cur_pos});
                }
                DataCommand::Rm { key } => {
                    if let Some(old_cmd) = key_dir.remove(&key) {
                        // 如果移除成功，那么说明之前的命令的值没有意义，增长无用字节数量
                        useless_size += old_cmd.value_size
                    }
                }
            }
            cur_pos = next_pos;
        }
        readers.insert(file_id, buf_reader); //将buf_reader插入到readers中
        return Ok(useless_size)
    }

    fn compact(&mut self,mut new_writer:BufWriterWithPos<File>) -> Result<()>{
        // 将现有数据全部写入到新的日志文件中
        for (key,cmd_pos) in self.key_dir.iter_mut() {
            let reader = self.readers.get_mut(&cmd_pos.file_id).expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.value_pos))?;
            if let DataCommand::Set {  value,.. } = serde_json::from_reader(reader.take(cmd_pos.value_size))?{
                let (value_pos,value_size) = new_writer.write_command(DataCommand::Set { key: key.to_owned(), value: value })?;
                cmd_pos.change(new_writer.file_id,value_size,value_pos); //写完后要更新命令位置
            }
        }
        self.writer = new_writer;
        // 将旧文件删除
        for (old_file_id,_) in &self.readers {
            let mut path_buf = self.data_dir.clone();
            path_buf.push(format!("{}.log", old_file_id));
            fs::remove_file(path_buf)?;
        }
        // 重新创建readers
        let mut new_readers = HashMap::new();
        let mut path_buf = self.data_dir.clone();
        path_buf.push(format!("{}.log", self.writer.file_id));
        let file = OpenOptions::new().read(true).open(path_buf)?;
        let buf_reader = BufReader::new(file);
        new_readers.insert(self.writer.file_id, buf_reader);
        self.readers = new_readers;
        // useless_size数据现在为0
        self.useless_size = 0;
        Ok(())
    }

    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path_buf: impl Into<PathBuf>) -> Result<KvStore> {
        let data_dir: PathBuf = path_buf.into();

        fs::create_dir_all(&data_dir)?;

        let mut key_dir = HashMap::new();
        let mut readers = HashMap::new();
        
        let mut log_file_list = Self::sorted_log_list(&data_dir);

        let writer = Self::get_writer(data_dir.to_owned(), &mut log_file_list)?;

        let mut useless_size = 0; //无用的数据量大小
        for &file_id in log_file_list.iter() {
            useless_size += Self::read_log_files(&mut key_dir, &mut readers, data_dir.to_owned(), file_id)?;
        }
        
        Ok(Self {
            key_dir,
            readers,
            writer,
            data_dir,
            useless_size: useless_size,
        })
    }
    
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()>{
        if self.useless_size > MAX_USELESS_SIZE {
            // 首先获取一个新的writer
            let mut log_file_list = Self::sorted_log_list(&self.data_dir);
            let new_writer = Self::get_writer(self.data_dir.clone(), &mut log_file_list)?;
            // 将现有的数据全部写到新文件中,并且清除旧数据
            self.compact(new_writer)?;
        }
        let (value_pos,value_size) = self.writer.write_command(DataCommand::Set { key: key.clone(), value: value })?;
        self.key_dir.entry(key)
            .and_modify(|x| {
                self.useless_size += x.value_size; // 增长无用字节数量
                x.change(self.writer.file_id,value_size,value_pos);
            })
            .or_insert(CommandPos{file_id:self.writer.file_id,value_size:value_size,value_pos:value_pos});
        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // 需要可变的self，因为我们会修改其readers的seek指针
        if let Some(cmd_pos) = self.key_dir.get(&key) {
            // 因为理论上这个log reader是必须存在的，所以用expect?
            let reader = self.readers.get_mut(&cmd_pos.file_id).expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.value_pos))?;
            // reader.take返回一个只能读取指定字节数量的读取器，这里返回的依然是一个可变引用
            // 使用..语法要求必须放在末尾，并且不能跟','
            if let DataCommand::Set {  value,.. } = serde_json::from_reader(reader.take(cmd_pos.value_size))?{
                return Ok(Some(value))
            }else {
                return Err(KvsError::UnexpectedCommandType);   
            }
        }else {
            // 此处没有对应的Key的逻辑是返回None,而Err(KvsError::KeyNotFound)应用在Remove中
            return Ok(None);
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(cmd_pos) = self.key_dir.get(&key) { //存在才需要删除，否则不删除
            let (_,_) = self.writer.write_command(DataCommand::Rm { key: key.clone()})?;
            self.useless_size += cmd_pos.value_size;
            self.key_dir.remove(&key);
            Ok(())
        }else{
            return Err(KvsError::KeyNotFound); 
       }
    }
}

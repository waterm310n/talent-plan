use serde::{Deserialize, Serialize};

/// keydir的值结构，记录命令位置
pub struct CommandPos {
    /// 说明文件id
    pub file_id: u64,    
    ///值的大小
    pub value_size: u64, 
    ///值在文件偏移
    pub value_pos: u64,  
}

impl CommandPos {
    /// 修改CommandPos的值,同时返回旧值的大小
    pub fn change(&mut self, file_id: u64, value_size: u64, value_pos: u64) -> u64 {
        let res = self.value_size;
        self.file_id = file_id;
        self.value_size = value_size;
        self.value_pos = value_pos;
        return res;
    }
}

/// 数据文件中的命令结构
#[derive(Serialize, Deserialize, Debug)]
pub enum DataCommand {
    /// Set命令
    Set {
        /// 键
        key: String,
        /// 值
        value: String,
    },
    /// Rm命令，移除Key
    Rm {
        /// 键
        key: String,
    },
}

impl DataCommand {
    /// 通过set方法构造Set数据命令
    pub fn set(key: String, value: String) -> DataCommand {
        Self::Set { key, value }
    }

    /// 通过rm方法构造Rm数据命令
    pub fn rm(key: String) -> DataCommand {
        Self::Rm { key }
    }
}

/// hint文件中的命令结构
pub struct HintCommand {
    key_size: u64,   //键的大小
    value_size: u64, //值的大小
    value_pos: String, //值在merged_file中的位置
    key: String,       //键
}

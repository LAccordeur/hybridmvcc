use crate::{transaction::Timestamp, Result};

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
    Error = 3,
}

impl From<u8> for TransactionStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::InProgress,
            1 => Self::Committed,
            2 => Self::Aborted,
            _ => Self::Error,
        }
    }
}

pub struct TransactionTable {
    table: BTreeMap<Timestamp, TransactionStatus>,
}

impl TransactionTable {
    pub fn new() -> Self {
        Self {
            table: BTreeMap::new(),
        }
    }

    pub fn get_transaction_status(&mut self, xid: Timestamp) -> Result<TransactionStatus> {
        match self.table.get(&xid) {
            Some(status) => Ok(status.clone()),
            None => Ok(TransactionStatus::InProgress),
        }
    }

    pub fn set_transaction_status(
        &mut self,
        xid: Timestamp,
        status: TransactionStatus,
    ) -> Result<()> {
        self.table.insert(xid, status);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_set_transaction_status() {
        let mut table = TransactionTable::new();

        for i in 0..100 {
            assert!(table
                .set_transaction_status(Timestamp::from(i), TransactionStatus::from(i as u8 % 4))
                .is_ok());
        }

        for i in 0..100 {
            let status = table.get_transaction_status(Timestamp::from(i)).unwrap();
            assert_eq!(TransactionStatus::from(i as u8 % 4), status);
        }
    }
}

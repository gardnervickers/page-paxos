//! A CASPaxos implementation over a fixed size address space.
//!
//! Each page in the address space is a CASPaxos register.
#![allow(dead_code)]
mod acceptor;
mod proposer;
mod sim;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Ballot(ProposerId, u32);

impl std::fmt::Display for Ballot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_unknown() {
            write!(f, "UNKNOWN")
        } else {
            write!(f, "{}.{}", self.0 .0, self.1)
        }
    }
}

impl std::fmt::Debug for Ballot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_unknown() {
            f.debug_tuple("Ballot").field(&"UNKNOWN").finish()
        } else {
            f.debug_tuple("Ballot")
                .field(&self.0 .0)
                .field(&self.1)
                .finish()
        }
    }
}

impl Ballot {
    fn new(id: ProposerId, ballot: u32) -> Self {
        Self(id, ballot)
    }

    fn increment(&self, id: ProposerId) -> Self {
        Self(id, self.1 + 1)
    }

    fn is_unknown(&self) -> bool {
        self == &Self::UNKNOWN
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self == other {
            return std::cmp::Ordering::Equal;
        }
        if self == &Self::UNKNOWN {
            return std::cmp::Ordering::Less;
        }
        if other == &Self::UNKNOWN {
            return std::cmp::Ordering::Greater;
        }
        // First compare the ballot numbers, fall back to the acceptor id.
        match self.1.cmp(&other.1) {
            std::cmp::Ordering::Equal => self.0.cmp(&other.0),
            ord => ord,
        }
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ballot {
    const UNKNOWN: Ballot = Self(ProposerId::UNKNOWN, u32::MAX);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ProposerId(u16);
impl ProposerId {
    const UNKNOWN: ProposerId = Self(u16::MAX);
    fn from_u16(id: u16) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Slot(u64);

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Versioned<T> {
    ballot: Ballot,
    value: Option<T>,
}

impl<T> std::fmt::Debug for Versioned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Versioned")
            .field("ballot", &self.ballot)
            .finish()
    }
}

impl<T> Versioned<T> {
    pub(crate) fn new(ballot: Ballot, value: Option<T>) -> Self {
        Self { ballot, value }
    }
    pub(crate) fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }

    pub(crate) fn into_value(self) -> Option<T> {
        self.value
    }

    pub(crate) fn ballot(&self) -> Ballot {
        self.ballot
    }
}

impl<T: Eq> std::cmp::Ord for Versioned<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ballot.cmp(&other.ballot)
    }
}
impl<T: Eq> std::cmp::PartialOrd for Versioned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ballot_order() {
        let acceptor1 = ProposerId::from_u16(1);
        let acceptor2 = ProposerId::from_u16(2);

        let ballot1 = Ballot(acceptor1, 1);
        let ballot2 = Ballot(acceptor2, 1);
        assert_ne!(ballot1, ballot2);
        assert!(ballot1 < ballot2);
    }

    #[test]
    fn unknown_ballot() {
        let acceptor1 = ProposerId::from_u16(1);
        let acceptor2 = ProposerId::from_u16(2);
        let ballot1 = Ballot(acceptor1, 1);
        let ballot2 = Ballot(acceptor2, 1);

        assert!(ballot1 != Ballot::UNKNOWN);
        assert!(ballot2 != Ballot::UNKNOWN);
        assert!(Ballot::UNKNOWN < ballot1);
        assert!(Ballot::UNKNOWN < ballot2);
    }
}

use thincollections::thin_map::ThinMap;
use crate::contract::StaticAddress;
use anyhow::{Result, Error, anyhow};
use crate::wip::Word;

/* Memory Layout

[subject_address] chair_person (assume read only so no conflicts)
[subject_address + 1] nb_proposals (<= m)
[subject_address + 2] proposal_0_name (32 bytes = 8 words)
[subject_address + 3] proposal_0_vote_count
...
[subject_address + 2 + (2 * i)] proposal_i_name (32 bytes = 8 words)
[subject_address + 3 + (2 * i)] proposal_i_vote_count
...
[subject_address + 2 + (2 * nb_proposals)] last_proposal_name (32 bytes = 8 words)
[subject_address + 3 + (2 * nb_proposals)] last_proposal_vote_count
//[end_address] = [subject_address + 4 + (2 * nb_proposals)]

[end_address + voter_1_address] voter_1_weight
[end_address + voter_1_address + 1] voter_1_delegate (None == StaticAddress::MAX)
[end_address + voter_1_address + 2] voter_1_vote (None == usize::MAX)
...
[end_address + last_voter_address] last_voter_weight
[end_address + last_voter_address + 1] last_voter_delegate (None == StaticAddress::MAX)
[end_address + last_voter_address + 2] last_voter_vote (None == usize::MAX)

Notes to make the workload
# voter_i_address + 2 < voter_j_address
# subject_i_address + 4 + (2 * nb_proposals) + last_voter_addr + 2 < subject_j_address
 */
struct VotingSubject {
    chair_person: StaticAddress,
    proposals: Vec<Proposal>,
    voters: ThinMap<StaticAddress, Voter>
}

#[derive(Debug)]
enum VotingError {
    PermissionDenied,
    UnknownProposal,
    UnregisteredVoter,
    AlreadyVoted,
    NonZeroWeight,
    NoVotingRights,
    SelfDelegation,
    DelegationLoop,
    NoProposals,
    NoVotes,
}

struct Voter {
    address: StaticAddress, // instead of mapping
    weight: u64,
    delegate: Option<StaticAddress>,
    vote: Option<usize>
}
impl Voter {
    fn new(address: StaticAddress, weight: u64) -> Self {
        Self {
            address,
            weight,
            delegate: None,
            vote: None,
        }
    }

    fn voted(&self) -> bool {
        self.vote.is_some()
    }
}

type ProposalName = [Word; 8];
struct Proposal {
    name: ProposalName,
    vote_count: u64
}

impl VotingSubject {
    pub fn new(sender: StaticAddress, proposal_names: Vec<ProposalName>) -> Result<Self, VotingError> {

        if proposal_names.is_empty() {
            return Err(VotingError::NoProposals);
        }

        let mut voters = ThinMap::new();
        voters.insert(sender, Voter::new(sender, 1));

        let proposals: Vec<_> = proposal_names.iter()
            .map(|name| {
                Proposal{
                    name: *name,
                    vote_count: 0,
                }
        }).collect();

        Ok(Self {
            chair_person: sender,
            proposals,
            voters
        })
    }

    pub fn give_right_to_vote(&mut self, sender: StaticAddress, voter_address: StaticAddress) -> Result<(), VotingError> {
        if sender != self.chair_person {
            return Err(VotingError::PermissionDenied);
        }

        match self.voters.get_mut(&voter_address) {
            None => {
                // Can give right to vote
                let new_voter = Voter::new(voter_address, 1);
                self.voters.insert(voter_address, new_voter);
                Ok(())
            },
            Some(voter) if voter.voted() => {
                // Can't give voting rights
                Err(VotingError::AlreadyVoted)
            },
            Some(voter) if voter.weight == 0 => {
                // Can give right to vote
                voter.weight = 1;
                Ok(())
            },
            _ => {
                Err(VotingError::NonZeroWeight)
            }
        }
    }

    pub fn delegate(&mut self, sender: StaticAddress, to: StaticAddress) -> Result<(), VotingError> {

        if sender == to {
            return Err(VotingError::SelfDelegation);
        }

        match self.voters.get_mut(&sender) {
            None => {
                Err(VotingError::UnregisteredVoter)
            }
            Some(voter) if voter.weight == 0 => {
                Err(VotingError::NoVotingRights)
            },
            Some(voter) if voter.voted() => {
                Err(VotingError::AlreadyVoted)
            },
            Some(voter) => {
                // Find final delegate
                let mut address_current_delegate = to;
                while let Some(address_next_delegate) = self.voters
                    .get(&address_current_delegate)
                    .ok_or(VotingError::UnregisteredVoter)?.delegate {

                    if address_next_delegate == sender {
                        return Err(VotingError::DelegationLoop)
                    }

                    address_current_delegate = address_next_delegate;
                }

                // Safety: This voter exists otherwise, the loop would have exited
                let delegate = self.voters.get_mut(&address_current_delegate).unwrap();
                if delegate.weight == 0 {
                    return Err(VotingError::NoVotingRights);
                }

                match delegate.vote {
                    Some(proposal_id) => {
                        let proposal = self.proposals.get_mut(proposal_id)
                            .ok_or(VotingError::UnknownProposal)?;
                        voter.delegate = Some(address_current_delegate);
                        voter.vote = Some(proposal_id);
                        proposal.vote_count += voter.weight;
                    },
                    None => {
                        voter.delegate = Some(address_current_delegate);
                        voter.vote = Some(usize::MAX);
                        delegate.weight += voter.weight;
                    }
                }

                Ok(())
            },
        }
    }

    pub fn vote(&mut self, sender: StaticAddress, proposal_id: usize) -> Result<(), VotingError> {
        let voter = self.voters.get_mut(&sender)
            .ok_or(VotingError::UnregisteredVoter)?;

        if voter.weight == 0 {
            return Err(VotingError::NoVotingRights);
        }

        if voter.voted() {
            return Err(VotingError::AlreadyVoted);
        }

        let proposal = self.proposals.get_mut(proposal_id)
            .ok_or(VotingError::UnknownProposal)?;

        voter.vote = Some(proposal_id);
        proposal.vote_count += voter.weight;

        Ok(())
    }

    fn winning_proposal(&self) -> Result<usize, VotingError> {
        let mut winning_count = 0;
        let mut winner = usize::MAX;

        for (proposal_id, proposal) in self.proposals.iter().enumerate() {
            if proposal.vote_count > winning_count {
                winning_count = proposal.vote_count;
                winner = proposal_id;
            }
        }

        if winner == usize::MAX {
            Err(VotingError::NoVotes)
        } else {
            Ok(winner)
        }
    }

    pub fn winner_name(&self) -> Result<ProposalName, VotingError> {
        // TODO people can still vote after the result has been announced...
        let winner_index = self.winning_proposal()?;
        let winner = self.proposals.get(winner_index)
            .ok_or(VotingError::UnknownProposal)?;
        Ok(winner.name)
    }
}
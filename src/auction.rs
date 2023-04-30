use thincollections::thin_map::ThinMap;
use crate::contract::{SharedMap, StaticAddress};
use crate::vm_utils::SharedStorage;

/* Memory layout:
[bidder_0_address] bidder_0_balance
...
[bidder_n_address] bidder_n_balance
[auction_address] beneficiary
[auction_address + 1] auction_balance_address
[auction_address + 2] end_time
[auction_address + 3] ended
[auction_address + 4] highest_bidder
[auction_address + 5] highest_bid
//[end_address] = [subject_address + 6]

[end_address + bidder_0_address] bidder_0_amount
...
[end_address + bidder_i_address] bidder_i_amount
..
[end_address + last_bidder_address] last_bidder_amount

Notes to make the workload
# bidder_n_address < auction_address
# subject_i_address + 6 + last_bidder_addr < subject_j_addr
 */
#[derive(Debug)]
pub struct SimpleAuction<'a> {
    pub beneficiary: StaticAddress,
    pub auction_balance_address: StaticAddress,
    pub end_time: u64,    // TODO
    pub ended: bool,
    pub highest_bidder: StaticAddress,
    pub highest_bid: u64,
    pub pending_returns: SharedMap<'a, u64>,
    // highest_bidder_increased: fn(StaticAddress, u64), // TODO
    // auction_ended: fn(StaticAddress, u64), // TODO
}

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Bid,
    Withdraw,
    Close
}

pub type Result = core::result::Result<Success, Error>;

#[derive(Debug, Clone, Copy)]
pub enum Success {
    // SuccessfulBid(StaticAddress, StaticAddress, u64),
    // Withdraw(StaticAddress, StaticAddress, u64),
    // CollectHighestBid(StaticAddress, StaticAddress, u64),
    Transfer(StaticAddress, StaticAddress, u64),
}

#[derive(Debug, Clone, Copy)]
pub enum Error {
    AuctionAlreadyEnded,
    BidNotHighEnough(u64),
    AuctionNotYetEnded,
    AuctionAlreadyClosed,
    UnknownBidder,
    NothingToWithdraw
}

impl<'a> SimpleAuction<'a> {
    pub fn new(bidding_time: u64, beneficiary: StaticAddress, auction_address: StaticAddress, shared_map: SharedMap<'a, u64>) -> Self {
        let now = 0;    // TODO
        Self {
            beneficiary,
            auction_balance_address: auction_address,
            end_time: now + bidding_time,
            ended: false,
            highest_bidder: beneficiary,
            highest_bid: 0,
            pending_returns: shared_map,
        }
    }

    pub unsafe fn bid(&mut self, bidder: StaticAddress, new_bid: u64) -> Result {
        // TODO There is no concept of time in this vm
        // let now = 0;
        // if now > self.end_time {
        //     return Err(AuctionError::AuctionAlreadyEnded);
        // }
        if self.ended {
            return Err(Error::AuctionAlreadyClosed);
        }

        if new_bid <= self.highest_bid {
            return Err(Error::BidNotHighEnough(self.highest_bid));
        }

        let previous_highest_bid = self.highest_bid;
        self.highest_bid = new_bid;
        let previous_highest_bidder = self.highest_bidder;
        self.highest_bidder = bidder;

        if previous_highest_bid > 0 {
            // Send money back to previous highest bidder
            if let Some(mut to_return) = self.pending_returns.get_mut(previous_highest_bidder) {
                *to_return += previous_highest_bid;
            } else {
                self.pending_returns.insert(previous_highest_bidder, previous_highest_bid);
            }
        }

        // emit HighestBidIncreased(bidder, new_bid);

        Ok(Success::Transfer(bidder, self.auction_balance_address, new_bid))
    }

    pub unsafe fn withdraw(&mut self, sender: StaticAddress) -> Result {
        let pending = self.pending_returns.get_mut(sender)
            .ok_or(Error::UnknownBidder)?;

        if *pending > 0 {
            let amount = *pending;
            *pending = 0;

            // TODO Send money back to the sender (new tx regardless of monolithic vs pieced)
            return Ok(Success::Transfer(self.auction_balance_address, sender, amount));
        }

        Err(Error::NothingToWithdraw)
    }

    pub fn close_auction(&mut self) -> Result {
        // TODO There is no concept of time in this vm
        // let now = 0;
        // if now < self.end_time {
        //     return Err(AuctionError::AuctionNotYetEnded);
        // }

        if self.ended {
            return Err(Error::AuctionAlreadyClosed);
        }

        self.ended = true;

        // emit AuctionEnded(highestBidder, highestBid);

        // TODO Send money to the beneficiary (new tx regardless of monolithic vs pieced)
        Ok(Success::Transfer(self.auction_balance_address, self.beneficiary, self.highest_bid))
    }
}
use thincollections::thin_map::ThinMap;
use crate::contract::StaticAddress;

/* Memory layout:
[bidder_0_address] bidder_0_balance
...
[bidder_n_address] bidder_n_balance
[auction_address] beneficiary
[auction_address + 1] end_time
[auction_address + 2] ended
[auction_address + 3] highest_bidder
[auction_address + 4] highest_bid
//[end_address] = [subject_address + 5]

[end_address + bidder_0_address] bidder_0_amount
...
[end_address + bidder_i_address] bidder_i_amount
..
[end_address + last_bidder_address] last_bidder_amount

Notes to make the workload
# bidder_n_address < auction_address
# subject_i_address + 5 + last_bidder_addr < subject_j_addr
 */
struct SimpleAuction {
    beneficiary: StaticAddress,
    end_time: usize,    // TODO
    ended: bool,
    highest_bidder: StaticAddress,
    highest_bid: u64,
    pending_returns: ThinMap<StaticAddress, u64>,
    // highest_bidder_increased: fn(StaticAddress, u64), // TODO
    // auction_ended: fn(StaticAddress, u64), // TODO
}

enum AuctionError {
    AuctionAlreadyEnded,
    BidNotHighEnough(u64),
    AuctionNotYetEnded,
    AuctionAlreadyClosed,
    UnknownBidder,
}

impl SimpleAuction {
    pub fn new(bidding_time: usize, beneficiary: StaticAddress) -> Self {
        let now = 0;    // TODO
        Self {
            beneficiary,
            end_time: now + bidding_time,
            ended: false,
            highest_bidder: beneficiary,
            highest_bid: 0,
            pending_returns: ThinMap::new(),
        }
    }

    pub fn bid(&mut self, bidder: StaticAddress, new_bid: u64) -> Result<(), AuctionError> {
        let now = 0;    // TODO
        if now > self.end_time {
            return Err(AuctionError::AuctionAlreadyEnded);
        }

        if new_bid <= self.highest_bid {
            return Err(AuctionError::BidNotHighEnough(self.highest_bid));
        }

        if self.highest_bid > 0 {
            // Send money back to previous highest bidder
            if let Some(mut to_return) = self.pending_returns.get_mut(&self.highest_bidder) {
                *to_return += self.highest_bid;
            }
        }

        self.highest_bidder = bidder;
        self.highest_bid = new_bid;

        // emit HighestBidIncreased(bidder, new_bid);

        Ok(())
    }

    pub fn withdraw(&mut self, sender: StaticAddress) -> Result<(), AuctionError> {
        let pending = self.pending_returns.get_mut(&sender)
            .ok_or(AuctionError::UnknownBidder)?;

        if *pending > 0 {
            let amount = *pending;
            *pending = 0;

            // TODO Send money back to the sender (new tx regardless of monolithic vs pieced)
            // => generate a Transfer transaction
        }

        Ok(())
    }

    pub fn close_auction(&mut self) -> Result<(), AuctionError> {
        let now = 0;
        if now < self.end_time {
            return Err(AuctionError::AuctionNotYetEnded);
        }

        if self.ended {
            return Err(AuctionError::AuctionAlreadyClosed);
        }

        self.ended = true;

        // emit AuctionEnded(highestBidder, highestBid);

        // TODO Send money to the beneficiary (new tx regardless of monolithic vs pieced)
        // => generate a Transfer transaction

        Ok(())
    }
}
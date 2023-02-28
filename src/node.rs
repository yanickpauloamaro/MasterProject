use hwloc::{Topology, CPUBIND_PROCESS, TopologyObject, ObjectType};

pub trait Node {

    fn process() {
        println!("Processing");
    }
}

pub struct BasicNode {

}

impl Node for BasicNode {

}

impl BasicNode {
    pub fn new() {

    }
}
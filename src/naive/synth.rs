use std::hash::Hash;

use crate::synth_comms::*;

pub struct Synth {}

impl ProblemDomain for Synth {
    type TSolution = Ast;

    // This should be a term bank subset
    type TBatchInput = Vec<TermGroup>;

    // This should be either a term bank subset or a proposed soln
    type TBatchOutput = Advancement;

    // This should be the leader's cumulative term bank
    type TGen = TermBank;

    // This should be a grammar + constraint inputs + goal signature
    type TSpecification = TermBank;

    fn MakeGen(spec: Self::TSpecification) -> Self::TGen {
        TermBank {
            spec: spec,
            stride: 10000,
        }
    }
}

#[derive(Clone,PartialEq)]
enum Advancement {
    Solution(Ast),
    Other(TermGroupSet),
}
use std::collections::{HashMap, HashSet};

use u32 as NonterminalId;
use u32 as CostLevel;

use super::aggregation::{ProblemDomain, Gen, ProblemId,BatchId};



struct Bank<TCost: Hash + Eq, TKey: Hash + Eq, TValue> {
    filter: HashSet<TKey>,
    levels: HashMap<TCost, Vec<(TKey, TValue)>>,
}

impl<TCost: Hash + Eq, TKey: Hash + Eq, TValue> Bank<TCost, TKey, TValue> {
    
    fn include<TIter: IntoIterator<Item = (TKey, TValue)>>(
        &mut self,
        cost_level: TCost,
        entries: TIter,
    ) {
        let levels_group = self.levels.entry(cost_level).or_insert(Vec::new());

        for (key, value) in entries {
            if self.filter.insert(key) {
                levels_group.push((key, value));
            }
        }
    }
}

struct TermBank {
    data: HashMap<NonterminalId,Bank<CostLevel,Vec<u32>,Ast>>,
}

impl Gen<Vec<TermGroup>> for TermBank {
    fn try_make(&self, batch_id:BatchId) -> Option<Vec<TermGroup>> {
        todo!()
    }
}



struct SynthBlockBatch {}

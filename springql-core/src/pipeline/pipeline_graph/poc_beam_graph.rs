use std::{cell::RefCell, fmt::Debug, rc::Rc};

use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::IntoNodeReferences,
};

type Graph = Rc<RefCell<DiGraph<PCollection, PTransform>>>;

fn apply_to_graph(graph: &Graph, src_idx: NodeIndex, transform: PTransform) -> PCollection {
    let out_pcollection = PCollection::new(transform.out_pcollection_name.clone(), graph.clone());
    let out_idx = graph.borrow_mut().add_node(out_pcollection.clone());
    let _edge = graph.borrow_mut().add_edge(src_idx, out_idx, transform);
    out_pcollection
}

#[derive(Debug, Default)]
struct Pipeline {
    graph: Graph,
    // TODO cache by HashMap<PCollection, NodeIndex>
}

impl Pipeline {
    fn apply(&self, read_transform: ReadTransform) -> PCollection {
        // many R's in a pipeline
        let root_pcol = PCollection::new("R".to_string(), self.graph.clone());
        let root_idx = self.graph.borrow_mut().add_node(root_pcol);
        apply_to_graph(&self.graph, root_idx, read_transform.into())
    }
}
impl From<ReadTransform> for PTransform {
    fn from(read_transform: ReadTransform) -> Self {
        PTransform::new(read_transform.out_pcollection_name)
    }
}

#[derive(Clone, new)]
struct PCollection {
    name: String,
    graph: Graph,
}
impl PartialEq for PCollection {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Debug for PCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // prevent print-loop from `graph` field
        f.debug_struct("PCollection")
            .field("name", &self.name)
            .finish()
    }
}

impl PCollection {
    fn apply(&self, ptransform: PTransform) -> PCollection {
        let in_idx = self.node_index();
        apply_to_graph(&self.graph, in_idx, ptransform)
    }

    fn node_index(&self) -> NodeIndex {
        self.graph
            .borrow()
            .node_references()
            .find_map(|(idx, pc)| (pc == self).then(|| idx))
            .unwrap()
    }
}

#[derive(Debug, new)]
struct PTransform {
    out_pcollection_name: String,
}

#[derive(Debug, new)]
struct ReadTransform {
    out_pcollection_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_() {
        let mut pipeline = Pipeline::default();

        let pcollection1 = pipeline.apply(ReadTransform::new("1".to_string()));
        let pcollection2 = pcollection1.apply(PTransform::new("2".to_string()));

        println!("{:#?}", pipeline);
    }
}

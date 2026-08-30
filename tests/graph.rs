use workflow_kernel::{DefinitionError, WorkflowDefinition};

#[test]
fn cycle_is_rejected() {
    let err = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .node("b", "e")
        .node("c", "e")
        .edge("a", "b")
        .edge("b", "c")
        .edge("c", "a")
        .build()
        .unwrap_err();
    assert_eq!(err, DefinitionError::Cycle);
}

#[test]
fn empty_graph_rejected() {
    let err = WorkflowDefinition::builder("wf").build().unwrap_err();
    assert_eq!(err, DefinitionError::Empty);
}

#[test]
fn disconnected_node_rejected() {
    // Undeclared endpoint — not weakly-connected (fan-out of independent
    // nodes is legal and covered in happy_path).
    let err = WorkflowDefinition::builder("wf")
        .node("a", "e")
        .edge("a", "ghost")
        .build()
        .unwrap_err();
    assert!(matches!(err, DefinitionError::DisconnectedNode(id) if id.as_str() == "ghost"));
}

#[test]
fn duplicate_node_id_rejected() {
    let err = WorkflowDefinition::builder("wf")
        .node("a", "e1")
        .node("a", "e2")
        .build()
        .unwrap_err();
    assert!(matches!(err, DefinitionError::DuplicateNode(id) if id.as_str() == "a"));
}

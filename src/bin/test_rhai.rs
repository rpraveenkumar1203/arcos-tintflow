fn main() {
    let ctx = serde_json::json!({ "value": 42 });
    let mut engine = rhai::Engine::new();
    let mut scope = rhai::Scope::new();
    
    let dynamic_ctx: rhai::Dynamic = rhai::serde::to_dynamic(ctx).unwrap();
    scope.push("ctx", dynamic_ctx);
    
    let script = r#"
        ctx.value = ctx.value + 1;
        ctx
    "#;
    
    let result: rhai::Dynamic = engine.eval_with_scope(&mut scope, script).unwrap();
    let output: serde_json::Value = rhai::serde::from_dynamic(&result).unwrap();
    println!("{}", output);
}

use hy_emit::emit_go;
use hy_syntax::parse_file;

fn emit_src(src: &str) -> String {
    let ast = parse_file("test.hy", src).expect("parses");
    emit_go(&ast).expect("emits")
}

// Issue #646: set/option builtins (T9/T20) passed `hyb check` but emitted
// unbuildable Go — Go's builtin `len`/`delete` reject the `set[…]` struct,
// and `option[…]` had no `unwrap`/`or`/`is_some` methods in the prelude.
// The fix routes set-typed `len`/`delete` to the SET_PRELUDE methods and
// adds the three option methods to the OPTION_PRELUDE.
#[test]
fn set_and_option_builtins_lower_to_buildable_go_issue_646() {
    let go = emit_src(
        "\
fn main() {
    seen := set[int]{}
    seen.insert(1)
    fmt.println(len(seen))
    delete(seen, 1)
    o := some(5)
    fmt.println(o.unwrap())
    fmt.println(o.or(0))
    fmt.println(o.is_some())
}
",
    );
    // len(seen) on a set[int] -> the prelude's .len() method, not Go's len().
    assert!(
        go.contains("seen.len()"),
        "expected len(seen) to lower to seen.len(), got:\n{go}"
    );
    // delete(seen, v) on a set[int] -> the prelude's .remove(v) method.
    assert!(
        go.contains("seen.remove(1)"),
        "expected delete(seen, 1) to lower to seen.remove(1), got:\n{go}"
    );
    // option methods must exist in the OPTION_PRELUDE.
    assert!(
        go.contains("func (o option[v]) unwrap() v"),
        "expected unwrap() method in the option prelude, got:\n{go}"
    );
    assert!(
        go.contains("func (o option[v]) or(d v) v"),
        "expected or(d) method in the option prelude, got:\n{go}"
    );
    assert!(
        go.contains("func (o option[v]) is_some() bool"),
        "expected is_some() method in the option prelude, got:\n{go}"
    );
    // No bare Go builtins left that would reject the structs.
    assert!(
        !go.contains("fmt.Println(len(seen))"),
        "len(seen) must not be emitted verbatim, got:\n{go}"
    );
    assert!(
        !go.contains("delete(seen, 1)"),
        "delete(seen, 1) must not be emitted verbatim, got:\n{go}"
    );
}

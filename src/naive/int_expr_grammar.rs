
const N_PROD: u64 = 6;

pub fn pretty_print(encoded: u64) -> String {
    let mut tokens:Vec<String> = Vec::new();
    
    if pretty_print_inner(&mut(encoded.clone()), &mut tokens).is_ok() {
        tokens.join(" ")
    } else {
        "<invalid>".to_owned()
    }
}

fn pretty_print_inner(encoded: &mut u64, tokens: &mut Vec<String>) -> Result<(),()> {
    if *encoded==0 {
        return Err(());
    }
    let rem: u64;
    (*encoded, rem) = (*encoded / N_PROD, *encoded % N_PROD);
    
    match rem {
        0 => tokens.push("1".to_owned()),
        1 => tokens.push("2".to_owned()),
        2 => tokens.push("x".to_owned()),
        3 => tokens.push("y".to_owned()),

        4 => {
            tokens.push("(".to_owned());
            pretty_print_inner(encoded, tokens)?;
            tokens.push("+".to_owned());
            pretty_print_inner(encoded, tokens)?;
            tokens.push(")".to_owned());
        } 

        5 => {
            tokens.push("(".to_owned());
            pretty_print_inner(encoded, tokens)?;
            tokens.push("*".to_owned());
            pretty_print_inner(encoded, tokens)?;
            tokens.push(")".to_owned());
        }
        _ => {
            return Err(());
        }
    }
    
    Ok(())
}

// Evaluate this term as an integer expression.
pub fn outer_eval(src_encoded: u64, x:i32, y:i32) -> Option<i32> {
    let mut hot = src_encoded;
    
    if let Some(value) = recurse_intexp(&mut hot, x,y)  {
        if hot == 0 {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
    
}

fn recurse_intexp( encoded:  &mut u64, x: i32, y: i32) -> Option<i32> {
    if *encoded == 0 {
        return None;
    }
    
    let rem: u64;
    (*encoded, rem) = (*encoded / N_PROD, *encoded % N_PROD);
    
    match rem {
        0 => Some(1),
        1 => Some(2),
        2 => Some(x),
        3 => Some(y),

        4 => recurse_intexp(encoded, x, y, )
            .and_then(|a| recurse_intexp(encoded, x, y, ).map(|b| a + b)),

        5 => recurse_intexp(encoded, x, y, )
            .and_then(|a| recurse_intexp(encoded, x, y, ).map(|b| a * b)),
        _ => None,
    }
}

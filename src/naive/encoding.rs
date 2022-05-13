// struct grammar {
//     prod_counts: Vec<u64>,
// }

// fn decode(grammar: &grammar, mut encoded: u64) -> Vec<u64> {
//     let n_prod = 5;
//     let mut a: Vec<u64> = Vec::new();

//     let mut rem;
//     loop {
//         (encoded, rem) = (encoded / n_prod, encoded % n_prod);
//         a.push(rem);
//         if encoded == 0 {
//             break;
//         }
//     }

//     a
// }

pub fn pretty_print(encoded: u64) -> String {
    let mut tokens:Vec<String> = Vec::new();
    
    if let Ok(_) = pretty_print_inner(&mut(encoded.clone()), &mut tokens) {
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
    (*encoded, rem) = (*encoded / n_prod, *encoded % n_prod);
    
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


pub fn outer_eval(src_encoded: u64, x:i32, y:i32) -> Option<i32> {
    let mut hot = src_encoded.clone();
    
    
    
    if let Some(value) = recurse_intexp(&mut hot, x,y)  {
        if(hot == 0) {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
    
}
const n_prod: u64 = 6;
pub fn recurse_intexp( encoded:  &mut u64, x: i32, y: i32) -> Option<i32> {
    
    
    
    if *encoded == 0 {
        return None;
    }
    
    let rem: u64;
    (*encoded, rem) = (*encoded / n_prod, *encoded % n_prod);
    
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

/*
3 1 2

3*16 + 1*4 + 2*1 = 48+4+2 = 54

54/4 = 13,2

13/4 = 3,1

3/4 = 0,3

*/

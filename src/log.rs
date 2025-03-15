// main.rs
// use::home::eyk::.rustup::toolchains::stable-x86_64-unknown-linux-gnu/lib/rustlib/src/rust/library/std/src/macros.rs
#[macro_export]
macro_rules! pppp {
    ($length:expr) => {
        let _func_call_name = "函数调用名称";
        let _start_end_func = "函数开始与结束功能";
        let _steps_in_func = "函数之中的步骤";
        let _data_content = "数据";
        let _block_char = "\u{25A0}"; // 填充字符

        // 直接在格式化字符串中插入填充字符
        println!(
            "\x1B[35m总声明: \x1B[0m\n\
            \x1B[31m红色=>{:■^width$}\x1B[0m\n\
            \x1B[32m绿色=>{:■^width$}\x1B[0m\n\
            \x1B[34m蓝色=>{:■^width$}\x1B[0m\n\
            \x1B[33m黄色=>{:■^width$}\x1B[0m",
            _func_call_name,
            _start_end_func,
            _steps_in_func,
            _data_content,
            // fill_char = _block_char,  // 填充字符
            width = $length           // 动态宽度
        );
    };
}

#[macro_export]
macro_rules! __function {
    () => {
        println!("\x1B[31m\n=>函数调用[{}:{}:{}]\n\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[31m\n=>函数调用[{}:{}:{}]{:#?}\n\x1B[0m",
                         file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($val:expr),+ $(,)?) => {
        ($(__function!($val)),+,)
    };
}

#[macro_export]
macro_rules! pppr {
    () => {
        println!("\x1B[31m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[31m[{}:{}:{}]{:#?}\x1B[0m",
                         file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($val:expr),+ $(,)?) => {
        ($(pppr!($val)),+,)
    };
}

// 这个宏仅仅是用来输出带颜色文字的，没有行列信息与换行
#[macro_export]
macro_rules! pppr_no_ln {
    () => {
        print!("\x1B\x1B[0m");
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                print!("\x1B{:#?}\x1B[0m",
                          &tmp);
                tmp
            }
        }
    };
    ($($val:expr),+ $(,)?) => {
        ($(pppr_no_ln !($val)),+,)
    };
}



#[macro_export]
macro_rules! ppr {
    () => {
        println!("\x1B[31m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[31m[{}:{}:{}] {:?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(ppr!($arg)),+,)
    };
}

#[macro_export]
macro_rules! pr {
    () => {
        println!("\x1B[31m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[31m[{}:{}:{}] {} = {}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pr!($arg)),+,)
    };
}

// 绿色相关宏
#[macro_export]
macro_rules! pppg {
    () => {
        println!("\x1B[32m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[32m[{}:{}:{}] {:#?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pppg!($arg)),+,)
    };
}

#[macro_export]
macro_rules! ppg {
    () => {
        println!("\x1B[32m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[32m[{}:{}:{}] {:?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(ppg!($arg)),+,)
    };
}

#[macro_export]
macro_rules! pg {
    () => {
        println!("\x1B[32m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[32m[{}:{}:{}] {} = {}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pg!($arg)),+,)
    };
}

// 蓝色相关宏
#[macro_export]
macro_rules! pppb {
    () => {
        println!("\x1B[34m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[34m[{}:{}:{}] {:#?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pppb!($arg)),+,)
    };
}

#[macro_export]
macro_rules! ppb {
    () => {
        println!("\x1B[34m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[34m[{}:{}:{}] {:?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(ppb!($arg)),+,)
    };
}

#[macro_export]
macro_rules! pb {
    () => {
        println!("\x1B[34m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[34m[{}:{}:{}] {} = {}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pb!($arg)),+,)
    };
}

// 黄色相关宏
#[macro_export]
macro_rules! pppy {
    () => {
        println!("\x1B[33m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[33m[{}:{}:{}] {:#?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(pppy!($arg)),+,)
    };
}

#[macro_export]
macro_rules! ppy {
    () => {
        println!("\x1B[33m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[33m[{}:{}:{}] {:?}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(ppy!($arg)),+,)
    };
}

#[macro_export]
macro_rules! py {
    () => {
        println!("\x1B[33m[{}:{}:{}]\x1B[0m", file!(), line!(), column!())
    };
    ($val:expr $(,)?) => {
        match &$val {
            tmp => {
                println!("\x1B[33m[{}:{}:{}] {} = {}\x1B[0m", file!(), line!(), column!(),  &tmp);
                tmp
            }
        }
    };
    ($($arg:expr),+ $(,)?) => {
        ($(py!($arg)),+,)
    };
}




// #[macro_export]
// macro_rules! ppr {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[31m{:?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! pr {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[31m{}\x1B[0m", $arg);
//         )*
//     };
// }

// // 绿色相关宏
// #[macro_export]
// macro_rules! pppg {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[32m{:#?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! ppg {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[32m{:?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! pg {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[32m{}\x1B[0m", $arg);
//         )*
//     };
// }

// // 蓝色相关宏
// #[macro_export]
// macro_rules! pppb {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[34m{:#?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! ppb {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[34m{:?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! pb {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[34m{}\x1B[0m", $arg);
//         )*
//     };
// }

// // 黄色相关宏
// #[macro_export]
// macro_rules! pppy {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[33m{:#?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! ppy {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[33m{:?}\x1B[0m", $arg);
//         )*
//     };
// }

// #[macro_export]
// macro_rules! py {
//     ($($arg:expr),*) => {
//         $(
//             println!("\x1B[33m{}\x1B[0m", $arg);
//         )*
//     };
// }



#[cfg(test)]
mod test{
    // use::std::;
    #[test]
    fn dbgtest(){
        // dbgpppr!("这是红色的调试信息!");
        // ppprr!("dddd");
        // ;
        pppr!("你好");
    }
    // dbg!();
}

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};

fn main() -> io::Result<()> {
    println!("빈로그 파일 경로를 입력하세요.");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    let binlog_path = input.trim();

    let mut file = File::open(binlog_path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let transaction_vec = content.split("BEGIN\n");

    let path = "./parse_row_format_binlog_result";
    match create_directory(path) {
        Ok(()) => println!("Directory created successfully."),
        Err(e) => eprintln!("Failed to create directory: {}", e),
    }

    for q in transaction_vec {
        let query_vec: Vec<String> = q
            .split('\n')
            .filter(|l| l.starts_with("###"))
            .map(|l| l.replace("###", "").trim().to_string())
            .collect();

        if query_vec.len() == 0 {
            continue;
        }

        let tmp: Vec<&str> = query_vec[0].split(' ').collect();
        let cmd = tmp[0];
        let table = tmp.last().unwrap();

        let mut query = query_vec[0].to_string();

        match cmd {
            "INSERT" => {
                let mut column_vec = vec![];
                let mut value_vec = vec![];

                query_vec[2..query_vec.len()].iter().for_each(|str| {
                    let tmp: Vec<&str> = str.split("=").collect();
                    column_vec.push(tmp[0]);
                    value_vec.push(tmp[1]);
                });
                query = query
                    + "(\n"
                    + &column_vec.join(",\n")
                    + "\n) VALUES (\n"
                    + &value_vec.join(",\n")
                    + "\n)";
            }
            "UPDATE" => {
                let mut iter = query_vec.iter();
                let mut where_vec: Vec<String> = vec![];
                iter.next();
                iter.next();
                while let Some(condition) = iter.next() {
                    if condition.starts_with("@") {
                        where_vec.push(condition.to_string());
                    } else {
                        break;
                    }
                }

                let mut set_vec: Vec<String> = vec![];
                while let Some(condition) = iter.next() {
                    if condition.starts_with("@") {
                        set_vec.push(condition.to_string());
                    } else {
                        break;
                    }
                }

                query =
                    query + "\nSET\n" + &set_vec.join(",\n") + "\nWHERE\n" + &where_vec.join(",\n");
            }
            "DELETE" => {
                let mut iter = query_vec.iter();
                let mut where_vec: Vec<String> = vec![];
                iter.next();
                iter.next();
                while let Some(condition) = iter.next() {
                    if condition.starts_with("@=") {
                        where_vec.push(condition.to_string());
                    } else {
                        break;
                    }
                }

                query = query + "\nWHERE\n" + &where_vec.join(",\n");
            }
            _ => {}
        }

        query = query + "\n;\n\n";

        let result = append_to_file(&(path.to_string() + "/" + table), query.as_str());
        match result {
            Ok(_) => {}
            Err(err) => println!("{}", err),
        }
    }

    println!("빈로그 파일 분석 완료");
    Ok(())
}

fn create_directory(path: &str) -> std::io::Result<()> {
    // 기존 폴더 삭제
    if fs::metadata(path).is_ok() {
        fs::remove_dir_all(path)?;
    }

    // 모든 하위 디렉터리 생성
    fs::create_dir_all(path)?;

    Ok(())
}

fn append_to_file(file_path: &str, content: &str) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true) // 파일이 없으면 생성
        .append(true) // 기존 내용에 이어서 쓰기
        .open(file_path)?; // 파일 열기
    file.write_all(content.as_bytes())?; // 데이터 쓰기
    Ok(())
}

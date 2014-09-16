
(* Real math functions
*)

(*
let rec cmp_reduce_lst cmp lst =
  match lst with
    [x] -> x
  | x::[y] -> cmp x y
  | x::xs -> cmp x (cmp_reduce_lst cmp xs);; 
let min_lst = cmp_reduce_lst min;;
let max_lst = cmp_reduce_lst max;;
*)

(* Sample mathematical functions *)

let rec fib2 x1 x2 n =
  if n == 1 then x1
  else if n == 2 then x2
  else (fib2 x1 x2 (n-2)) + (fib2 x1 x2 (n-1));;

let rec fib3 x1 x2 n =
  if n == 1 then x1
  else fib3 x2 (x1+x2) (n-1);;

let rec range a b =
    if a > b then [] (* comment *)
    else a :: range (a+1) b;;

let sum lst = List.fold_left (+) 0 lst;;

(* String and file functions *)

let read_all_lines file_name =
  let in_channel = open_in file_name in
  let rec read_recursive lines =
    try
      Scanf.fscanf in_channel "%[^\r\n]\n" (fun x -> read_recursive (x :: lines))
    with
      End_of_file ->
        lines in
  let lines = read_recursive [] in
  let _ = close_in_noerr in_channel in
  List.rev (lines);;

let str_split delim txt =
  let rest_of_str str = String.sub str 1 ((String.length str)-1) in
  let rec split_rec whole_words part_word rest_str =
    if (String.length rest_str)==0  && (String.length part_word)==0 then whole_words
    else if (String.length rest_str)==0 then (whole_words@[part_word])
    else
      let head = (String.sub rest_str 0 1) in
      let tail = (rest_of_str rest_str) in
      if (String.compare head delim == 0) then
        (split_rec (whole_words@[part_word]) "" tail)
      else
        let new_part = part_word ^ head in
        (split_rec whole_words new_part tail)
  in split_rec [] "" txt;;

(* Final Actions *)

let delim = " ";;
let line = "hello world";;
str_split delim line;;

let fname = "numbers.txt";;
let lines = read_all_lines fname;;
let broken_lines = List.map (str_split " ") lines;;
let broken_lines_as_ints = List.map (List.map int_of_string) broken_lines;;
let ncols = List.length (List.nth broken_lines_as_ints 0);;
let colsums =
  let ith_selector i = function parts -> List.nth parts i in
  let ith_elems i = List.map (ith_selector i) broken_lines_as_ints in
  let ith_sum i = List.fold_left (+) 0 (ith_elems i) in
  List.map ith_sum (range 0 (ncols-1));;
let myprint x =
  let _ = print_string "\n" in
  print_int x;;

print_string "Column sums:";;
List.map (function n -> let _ = print_string "\n" in print_int n) colsums;;
print_string "Done with sums\n";;

print_int (fib2 1  1 13);
print_string "\n";
print_int (fib3 1  1 13);
print_string "\n";
print_string "Hello world!\n";

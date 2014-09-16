
executable_name="foo"
fname="hello"

ocamlc -o $executable_name $fname.ml

#chmod 777 $executable_name

./$executable_name

rm $fname.cmo
rm $fname.cmi
rm $executable_name


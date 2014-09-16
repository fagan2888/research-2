% sample code

adjacent(1,2).
adjacent(2,3).
adjacent(3,4).
adjacent(a,b).
adjacent(b,c).
adjacent(c,d).

% inference

adjacent(A, B) :- adjacent(B, A).
connected(A, B) :-
    adjacent(A, B).
connected(A, B) :-
    adjacent(A, C),
    connected(C, B).

hello_world :- write('hello world!').

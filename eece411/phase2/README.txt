how to compile
javac -d . *.java

how to run
java phase2Pack.Server
java phase2Pack.Client Leons-MacBook-Air.local 5000 1


lots of things to be done. right now key and value are hard coded. key is converted to string for cuz im lazy.
I did a put()
also, right now the client keeps the connection open till it receives things from server(i took code from assign1)
but server won't send anything back to client
and server keeps open as long as client is open.. so to end both of them, go ctrl+C

we probably need to do epdimic to propragate the hashmap... that sooo much work ....

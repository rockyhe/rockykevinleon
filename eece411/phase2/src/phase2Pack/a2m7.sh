javac -d . *.java
jar cfm Server.jar Manifest.txt phase2Pack/*.class
scp Server.jar a2m7@ssh.ece.ubc.ca:~/

javac -d . *.java|jar cfm Server.jar Manifest.txt phase2Pack/*.class && scp -i ~/.ssh/id_rsa Server.jar a2m7@ssh.ece.ubc.ca:~/ && scp -i ~/.ssh/id_rsa nodeList.txt a2m7@ssh.ece.ubc.ca:~/

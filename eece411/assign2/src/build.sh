#!/bin/bash
javac -cp . com/matei/eece411/A2/*.java
rmic -classpath . com.matei.eece411.A2.ChatImpl
rmic -classpath . com.matei.eece411.A2.CallbackImpl
#!/bin/bash
javac -cp . com/v3l7/eece411/A2/*.java
rmic -classpath . com.v3l7.eece411.A2.ChatImpl
rmic -classpath . com.v3l7.eece411.A2.CallbackImpl

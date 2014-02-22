Team member:
Leon Lixing Yu, 75683094
Hui Tan   52648094
Rocky He 	74963091

1. Standout feature:
We displays all the data in LINE CHART, and interactive tables, using Google’s API, see description below. Since the x-axis font has lower bond limitation, x-axis cannot display the NAMES of all the nodes. But you can move the cursor along the chart to see results for all the nodes. 
The program uses Cron Job to periodically run backend parser to keep the data recentness. Also, since UI are developed in JavaScript, Google Viz are used to convert the data into JavaScript Object Notation (JSON). 		 								
Google Visualization (Viz) is an open-source tool used to create charts and statistical graphs on web pages. Since Viz can translate Python data structure to JavaScript Object Notation (JSON), the tool is the ideal bridge between backend data parser written in Python and frontend UI written in JavaScript. It receives large amount of data organized by Python, converts it to (JSON), and presents the JSON-formatted data as charts and analytical graphs on the web page. All the conversion are done in JavaScript in index.html. 							
		
Crobtab -e notation, running program every 5 hours:
* */5 * * * /PATH/TO/Stats.py 

2. Design Choice and backend parser architecture (in Python)
we implemented hierarchical infrastructure for the monitoring service. We use a ECE server as an always-on head machine. It periodically runs the program everyday to keep the statistics updated by using cronjob. the head machine runs a python program and its paramiko module to ssh each node and extract status: disk usage,disk free,node uptime. We have three scenarios for connection failures: 1. node offline 2. authentication failure 3.login failure.  We use python exception handler to provide unique feedback. the failed node are all presented in the server URL. 

3. Front End UI Program (in JavaScript and HTML)
Since JSON is the default data structure for JavaScript, and Google Viz’s graphics feature is fully compatible with JavaScript, I choose HTML and JavaScript to develop the UI.  See below for UI flowchart. 
start -> JSON data from results.txt-> set configuration of the charts such as legends, color,size-> set web page layout such as text font, color, size, and location-> data structure & charts configuration are sent to google Viz API->display data in chart in the browser. 	
	
6. How to files are connected:
stats.py ->(main program, finish parsing and converting to JSON) -> dump results to results.txt -> index.html -> (include stats-o-matrics.css and jQuery library) -> calls results from results.txt -> formatting the results for google viz -> browser calls index.html thru the URL. 
stats.py also uses nodeList.txt for our slices. 

7. results
we parse disk used, disk available, node liveness, and uptime in 161 nodes, and display all the status in line chart from the URL provided below. 	 	 	 		
The results are displayed on the following link:
http://www.ece.ubc.ca/~a2m7/

8.  Limitation 
  Since the program is running on cronjob, it displays periodic results instead of instantaneous result.  If the ece server(our head server) is offline, our program will stop functioning. 



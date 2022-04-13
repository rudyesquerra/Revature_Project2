### Requirements:
- Create a Spark Application that processes COVID data
- Your project 2 pitch should involve some analysis of COVID data. This is the central feature.
- Send me a link to a git repo, have someone in the group manage git (but they can ask for help from me)
- Produce one or more .jar files for your analysis. Then you can run application using spark-submit (in Ubuntu).
- 10 queries per group
- find a trend
- implement logging (with Spark)
- use Zeppelin (graphics and visuals) for showing trends and data analysis
- Implement Agile Scrum methodology for project work (choose Scrum Master who will serve as team lead, all communication with me funneled through this associate, and have daily scrum meetings, by end of day report blockers)

### Presentations
- Bring a simple slide deck providing an overview of your results. You should present your results, a high level overview of the process used to achieve those results, and any assumptions and simplifications you made on the way to those results.
- I may ask you to run an analysis on the day of the presentation, so be prepared to do so.
- We'll have 20 minutes per group, so make sure your presentation can be covered in that time, focusing on the parts of your analysis you find most interesting.
- Include a link to your github repository at the end of your slides


### Technologies

- Apache Spark
- Spark SQL
- YARN
- HDFS and/or S3
- SBT
- Scala 2.12.10
- Git + GitHub
- Zeppelin (or Tableau or other visualization software)


### Due Date
- Presentations will take place on Monday, 4/13
- Send Project proposal with MVP clearly labeled with 2-3 stretch goals by end of week, 4/1

### Contributors
- Evan Laferriere
- Rudy Esquerra
- Patrick Froerer
- Youngjung Kim
- Douglas Lam

<h3>Getting Started</h3>
How to run the app using spark-submit in WSL-Ubuntu

<ul>
<li>Start the Hadoop DFS daemons, the namenode and datanodes with => <i>startdfs</i></li>
<li>Start the resource manager with => <i>startyarn</i></li>
<li>create src/main/source directories in your home directory (/home/username/src/main/source) using the following command: <br>
<i>hdfs dfs -mkdir -p src/main/source</i></li>
<li>Jump into the directory where the jar is located (e.g. /mnt/c/Users/username/Desktop)</li>
<li>run the following command => <i>spark-submit --class QueryTesting project2_2.12-0.1.0.jar</i>
</ul>

PURPOSE:
	A parser that reads a csv file and inserts valid row into a sqlite database.

To start the program:
	Open project in IntelliJ
	Run CSVParser.java
	Follow console prompt (absolute file path or relative path if inside project folder)
	
	Output files are written to project-folder

Overview of approach, design choices, and assumption:
	BufferedReader was used to read file because it has high performance and easy to implement.
	String split with regex to ensure not including comma inside quotation marks although this will also include empty string.
		I used a function to check if there's any empty string in each row.
	prepareStatement was used because it has good performance and security.
	each statement is added to batch to avoid multiple individual insert query which would heavily impact performance.
	Number of inserts per transaction is limited to 100,000. Actual transaction size might be much larger.
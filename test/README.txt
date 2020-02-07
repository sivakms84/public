Source code : 

Source code is in file with the name "ScreenTemperature_Ranked.py" under project "greenflag".

inPath and outPath are command-line arguments that need to pass to the program.


Assumptions:

Only below three are requirements on this data. So removed all unwanted columns while writing into the parquet file.
- Which date was the hottest day?
- What was the temperature on that day?
- In which region was the hottest day?

Parquet data file should be easily queried by somebody and get answers for the above queries.
So ranked based on ScreenTemperature.

Test Cases:

Simple queries written by reading parquet file just to show the output.
It is not part of the requirement. You can consider them as test cases.

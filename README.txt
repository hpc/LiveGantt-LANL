# LiveViz 
A tool that generates PNG gantt charts for an HPC cluster at a set resolution & timeframe when called. 

Requirements:
* This tool will be launched via cron or some external means & therefore must be executable from the command line with no user interaction required
* This tool will produce a series of PNG artifacts that will then be displayed using "BrightLine". Because of this, this tool does not need to address the display of these charts. 
* This tool will produce visualizations from the sacct out dump
* This tool should account for differences between cluster configuration and outfile formatting 
* Produced images are ordered in the directory via a timestamp, sequence # or collation which corresponds to time.
* Produced images should be scaled appropriately for display on the intended media
* Tool is relatively compact and has minimal dependencies. 

Outline:
Shell script will be executed by cron, which will generate the sacct dump then launch the program. Python will be used due to it's operability with the BatViz tooling. If necessary, Rust with Python callings could be used in order to provide an executable binary, but this option is less ideal. The program will import the modified evalys tooling and batvis and make use of existing methods in order to minimize the amount of new work required and streamline the operation and integration of these tools.

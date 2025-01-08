
This Python code used by the Nsight Systems applications. This includes both
the "nsys" CLI application and the "nsys-ui" desktop application.


The "<nsys_install_dir>/reports" directory contains Python code used by the
stats and reporting feature.

The "<nsys_install_dir>/rules" directory contains Python code used by the
expert system feature.


These Python files are designed to be used as plug-in modules for the Nsight
Systems applications. THESE FILES ARE NOT DESIGNED TO BE RUN AS STANDALONE
SCRIPTS. Although it is possible to run some of these Python files directly,
this use-case is for building and testing, and not supported for general
customer use.



NSYS-UI DESKTOP APPLICATION
---------------------------

To utilize these reports and rules in the "nsys-ui" desktop application,
please utilize the drop-down menu in the lower-right section of the
application window to select either stats or expert systems. Once that
pane is visible, you an select a specific report or rule from the list
on the left side of the pane. The results will be displayed on the right
side of the pane.



NSYS COMMAND LINE APPLICATION
-----------------------------

To run a stats report, please use the command:

  $ nsys stats --report <report name> <input file>

To run an expert systems rule, please use the command:

  $ nsys analyze --rule <rule name> <input file>

The <report name> or <rule name> is the name of the Python script without
the ".py" extension.

To get a full list of the available reports & rules, the following commands
can be used:

  $ nsys stats --help-reports
  $ nsys analyze --help-rules

For more detailed information about a specific report, use the commands:

  $ nsys stats --help-reports <report name>
  $ nsys analyze --help-rules <rule name>

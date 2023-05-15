# # An Introduction to the CML Native Workbench

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Setup environment
from env import S3_ROOT, S3_HOME
import os
os.environ["S3_ROOT"] = S3_ROOT
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# ## Entering Code

# Enter code as you normally would in a Python shell or script:

print("Hello, Data Scientists!")

2 + 2

import math
def expit(x):
    """Compute the expit function (aka inverse-logit function or logistic function)."""
    return 1.0 / (1.0 + math.exp(-x))
expit(2)

import seaborn as sns
iris = sns.load_dataset("iris")
sns.pairplot(iris, hue="species")


# ## Getting Help

# Use the standard Python and IPython commands to get help:

help(expit)

expit?

#expit??



# ## Accessing the Linux Command Line

# Run a Linux command by using the `!` prefix.

# Print the current working directory:
!pwd

# **Note:** All CML Native Workbench users have the username `cdsw`.

# List the contents of the current directory:
!ls -l

# List the contents of the `/duocar` directory in the Hadoop Distributed File
# System (HDFS):
!hdfs dfs -ls $S3_ROOT/duocar

# You can also access the command line via the **Terminal access** menu item.


# ## Working with Python Packages

# Use `pip` in Python 2 or `pip3` in Python 3 to manage Python packages.

# **Important:** Packages are managed on a project-by-project basis.

# Get a list of the currently installed packages:
!pip3 list

# Install a new package:
!pip3 install folium

# **Note:** This package is now available in all future sessions launched in
# this project.

# Show details about the installed package:
!pip3 show folium

# **Note:**  This returns nothing if the package is not installed.

# Import the package:
import folium

# Use the package:
folium.Map(location=[46.8772222, -96.7894444])

# Uninstall the package:
#```python
#!pip3 uninstall -y folium
#```

# **Note:** Include the `-y` option to avoid an invisible prompt to confirm the
# uninstall.

# **Important:** This package is no longer available in all future sessions
# launched in this project.


# ## Formatting Session Output

# Use [Markdown](https://daringfireball.net/projects/markdown/syntax) text to
# format your session output.

# **Important:** Prefix the markdown text with the comment character `#`.

# ### Headings

# # Heading 1

# ## Heading 2

# ### Heading 3

# ### Text

# Plain text

# *Emphasized text* or _emphasized text_

# **Bold text** or __bold text__

# `Code text` (Note these are backtick quotes.)

# ### Mathematical Text

# Display an inline term like $\bar{x} = \frac{1}{n} \sum_{i=1}^{n} x_i$ using
# a [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics) expression
# surrounded by dollar-sign characters.

# A math expression can be displayed set apart by surrounding the LaTeX
# shorthand with double dollar-signs, like so: $$f(x)=\frac{1}{1+e^{-x}}$$

# ### Lists

# Bulleted List
# * Item 1
#   * Item 1a
#   * Item 1b
# * Item 2
# * Item 3

# Numbered List
# 1. Item 1
# 2. Item 2
# 3. Item 3

# ### Links

# Link to [Cloudera](http://www.cloudera.com).

# ### Images

# Display a stored image file:
from IPython.display import Image
Image("exercises/code/resources/spark.png")

# **Note:** The image path is relative to `/home/cdsw/` regardless of script
# location.

# ### Code blocks

# To print a block of code in the output without running it, use a comment line
# with three backticks to begin the block, then the block of code with each
# line preceded with the comment character, then a comment line with three
# backticks to close the block. Optionally include the language name after the
# opening backticks:

#```python
#print("Hello, Data Scientists!")
#```

# You can omit the language name to print the code block in black text without
# syntax coloring, for example, to display a block of static data or output:

#```
#Hello, Data Scientists!
#```

# ### Invisible comments

#[//]: # (To include a comment that will not appear in the)
#[//]: # (output at all, you can use this curious syntax.)

# Move along, nothing to see here.


# ## Exercises

# (1) Experiment with the CML Native Workbench command line.

# * Type a partial command and then press the TAB key.  Use the UP and DOWN
# ARROW keys to navigate the tab completion options.  Press the RETURN key to
# select a tab completion option.

# * Use the LEFT and RIGHT ARROW keys to navigate the command line.

# * Use the UP and DOWN ARROW keys to navigate the command line history.

# * Use SHIFT-RETURN to enter a multi-line command.

# (2) Experiment with the CML Native Workbench file editor.

# * Create a new file called `logit.py`.

# * Enter the following text and code into the file. **Note:** Do not enter
# the lines with three backticks. Delete the first comment character from each
# line.

#```python
## # Define and Evaluate the Logit Function
#
## Define the logit function:
#import numpy as np
#def logit(x): return np.log(x/(1.0-x))
#
## Evaluate the logit function:
#p = np.array([0.1, 0.25, 0.5, 0.75, 0.9])
#logit(p)
#```

# * Select a line or block of code and run the selection.

# * Clear the console log and run the entire file.

# (3) Experiment with the Linux command line.

# * Click on **Terminal access** to open a terminal window.

# * Use the `hdfs dfs -ls` command to explore the `/duocar/raw/` directory in
# HDFS.


# ## References

# [Cloudera Data Science Workbench](https://docs.cloudera.com/documentation/data-science-workbench/latest.html)

# [Markdown](https://www.markdownguide.org)

# [LaTeX](https://en.wikibooks.org/wiki/LaTeX/Mathematics)

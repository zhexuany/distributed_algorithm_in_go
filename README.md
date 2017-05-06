# MIT-6.824-2017
Checkout the offical website here: http://nil.csail.mit.edu/6.824/2017/schedule.html
You can find all lecture materials related with this course. 

You also need a api to submit you work and verify the correctness. Luckily enough, even for outside people you can create api key and submit your work.
The website is below:
https://6824.scripts.mit.edu/2017/handin.py/

## How to work with this course? ##
The first step is always clone project by running the commands below.
~~~
$ git clone git://g.csail.mit.edu/6.824-golabs-2017 6.824
$ cd 6.824
$ ls 
~~~
Of course you can change `6.824` to any name you want this project be.

Before you move on, ask yourself a question: Have you worked with `Go` project before?
If the answer is no. 
* set you `GOPATH` to current dir by running command `export "GOPATH=$PWD"`
If the answer if yes. I believe you know where this project goes. I leaves this to you.


## Pre flight check ##

Please do remember that if run all test before you submitting. The command for runing all tests is as follow:
~~~
bash ./test-mr.sh
~~~

If all tests passed, then you are ready to submit you work. Support you just finished your work about `lab1`, then you can
~~~
make lab1
~~~

It will ask you submit you work or not during the process, you can answer `no` in case you do not want to submit your work right now.


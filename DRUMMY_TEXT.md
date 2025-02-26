# Instruction

- After move file to path of server to position

## Schedule the Batch File with Task Scheduler

- Win + R
- type "taskschd.msc"
- Create a New Task follow your prefer.

```
In Task Scheduler, click Create Basic Task.
Name the task (e.g., "Run Cargo at 02:00") and give it a description if desired.
Select Daily and set it to repeat every day.
Set the start time to 02:00 for the first task.
For the Action, choose Start a Program and browse to the location of the run_cargo.bat file.
Click Finish.
```

*** Don't forget to test with manual run .bat file
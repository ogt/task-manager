Task Manager Service
====================

Service provides the basic support for a multi-tasking pool of workers, their manager as well as the business users that are submitting tasks that need to be done.

# /tasks

## POST /tasks

```javascript
{ 
    title : 'email me some positive thoughts. I am feeling down!'
      /*title or short description of the task
      displayed prominently in task listings and such
      some listing display shorter portions if you put a long title
      expect to be shown in many places with ellipsis

      required */
    description :  ''
     /* longer/full description of the task
      typically displayed when showing a single task

      optional 
      empty */

    deadline :  date
      /*deadline for the task
      datetime in whatever is the standard form an API

      optional  
      default to 24 hrs from submission time
      negative or 0 means asap and changes into now (plus + estimate)*/
  
    estimate : 300  //assuming that the units is "seconds"
      /*submitter expectation of time that it would take to finish the task
      in whatever is the standard unit for time apis

      optional
      default => 1 hr*/

    owner :  'odysseas@ezhome.com'
      /*who this task is for
      using email because it makes the API more easy to use..everyone has the email
      quite possibly internally the app should always be recording user-id  but allow
      incoming user references to be email based and outgoing user references are also 
      converted into the "user's current email" for api friendlyness

      optional
      default : the currently authenticated user*/

    tags : 'tag1 tag2'
     /*string of tags separated by whitespace
      optional 
      tags are used as the catch-all instead of structured filtering (list-ing) for now*/
      
    state : 'current task state'
     /* one of these string values:
        'queued'
        'worked on'
        'deleted'
        'done'*/
}
```

In response the POST call returns the newly created object
which includes filled in defaulted empty attributes, (equiv to GET /tasks/5768)

The above should be support the use case of the add command.

## GET /tasks?status=status&amp;owner=owner&amp;n=N

```javascript
[
    {
        description: 'description string',
        deadline: date,
        estimate: ,
        owner: 'owner email',
        properties: {key: value},
        state: 'queued/worked on/deleted/done',
        _id: task_id,
        _created_by: 'creator email',
        _created_on: date,
        _last_modified_on: date,
        _worked_by: 'worker email',
        _worked_on: date,
        _work_status: 'active/suspended/success/failure',
        _completed_on: date,
        _tags: 'first_tag second_tag'
    },
    ...
]
```
Returns list of N tasks with the given status and owner (status must be one of the following strings: 'queued/active/suspended/completed/deleted')


## /tasks/{task_id}

### GET /tasks/{task_id}

```javascript
{
  title : 'email me some positive thoughts. I am feeling down!'
  description  :
  deadline  : '2015-11-20T17:31:33Z'
  estimate : 300
  owner :  'odysseas@ezhome.com'
  tags : 'tag1 tag2'

  /* system attrs */
  _id : task_id
  _created_by : 'odysseas@ezhome.com'
  _created_on : '2015-11-20T17:26:33Z'
  _last_modified_on : '2015-11-20T17:26:33Z'
  _state : 'queued'
  _worked_by : 'mary@ezhome.com'            // if the task is under the control of a worker which worker that is
  _worked_on : '2015-11-20T17:26:33Z'       // if the task is under the control of a worker .. when it was last grabbed */
  _work_status : suspended or active        // if the task is under the control of a worker
  _completion_status : success or failure   // if the test is completed
  _completed_on : '2015-11-20T17:26:33Z'    // if the test is completed

}
```
Returns task information

### PUT /tasks/{task_id}

```javascript
{   
  _id : 5768
  title : 'email me some positive thoughts. I am feeling down!'
  description  :  
  deadline  : '2015-11-20T17:31:33Z'
  estimate : 300
  owner :  'odysseas@ezhome.com'

}
```
=> returns the updated object same as GET /tasks/5768

Updating an existing task
with PUT or whatever is the right convention for the update of an existing object

Given the minimal need for this I am trying to keep the implementation as simple as possible.

For simplicity, the complete object is being replaced,
ie, if a parameter is missing we do not keep existing values, we simply follow the same defaulting 
logic that we follow on `add` to generate defaults for the missing parameters.
Also, note that for now we do not allow tag update (more complex will need tag add and tag remove functionality)

The only "business logic" is that we only allow a task to be updated if it is in `queued` state.
Descriptive error messages are generated otherwise, e.g.
```
Cannot Update. Task is  (being worked on|completed|deleted)
Task does not exist.
```
The above should be able to support the use case of update command

### DELETE /tasks/{task_id}
It sets the state to "deleted"  - which is considered a final state<br/>
Only queued tasks are allowed to be deleted

``` 
returns the deleted object same as GET /tasks/5768
```

Descriptive error messages are generated on error
```
Cannot Delete. Task is  (being worked on|completed)
Task does not exist.
```
### /tasks/{task_id}/purge

#### POST /tasks/{task_id}/purge

Removes task and all events for this task from database

### /tasks/{task_id}/history

#### GET /tasks/{task_id}/history

This command shows chronological history of events and some additional timing information for selected task

```javascript
{
  events : [// complete chronoligical history of events
    { when : '2015-11-20T17:31:33Z', who : 'joe@ezhome.com', what : 'grab' },
    { when : '2015-11-20T17:31:33Z', who : 'joe@ezhome.com', what : 'grab' },
    { when : '2015-11-20T17:31:33Z', who : 'joe@ezhome.com', what : 'grab' },
    { when : '2015-11-20T17:31:33Z', who : 'joe@ezhome.com', what : 'grab' },
    { when : '2015-11-20T17:31:33Z', who : 'joe@ezhome.com', what : 'grab' },
  ]

  // some aggregates on top of the history of the events
  time_elapsed       // time since first submitted
  time_til_deadline
  time_worked_on     //time the task has spent being the active task of the current worker (only if in-progress)
  time_suspended     // time the task has spent suspended by the current worker (only if in-progress)
  time_queued        // total time in the queue
  time_rejected      // total time under the control of users that eventually released the task without completing it
  times_rejected     // how many times the current task has been rejected by prior users
}
```
## /tasks/queued

### GET /tasks/queued

Returns a list of tasks, which state is "queued". Maximum 100 tasks
```
returns array in the same format as GET /tasks
```
## /tasks/available

### GET /tasks/available?n=N

Returns a list of tasks ordered (1st most urgent ) up to N
if N missing returns just 1
In the simplest form it can return tasks based on order requested
```
returns array in the the same format as GET /tasks
```
# /workers

## POST /workers

```javascript

request:
{
  id : 'odysseas@ezhome.com'
}
returns:
if user tries to login from slack
{
    text: 'You are now logged in! You can start working.'
}
else
{
  token: uuid
}
```

Login current user


## GET /workers

```javascript
[
  {
    _id : 4567  // a worker-id
    userid : 'odysseas@ezhome.com'
    loggedin_on : date
    status: 'loggedout/loggedin'
  },
  ...
]
```
Returns a list of users from the database

## DELETE /workers

```
if user is working from slack
{
    text: 'You have just logged out! See you again'
}
```
```
else returns empty object
```
Logs out current user

## /workers/tasks

### POST /workers/tasks

Loads user's tasks from database and sets the first task active ("foreground" state)
```
returns fully filled task item
```

## /workers/tasks/{task_id}

### POST /workers/tasks/{task_id}

Sets selected task active ("foreground" state). If selected task is not in queue then throws error message
```
returns fully filled task item
```

### PUT /workers/tasks/{task_id}

Updates task status
If selected status is "fg" then task becomes active
If status is "bg" then task becomes suspended
If status is "release" then task returns to the task queue without completing
If status is "complete-success" then task becomes successfully completed
If status is "complete-failure" then task becomes completed(but with failure)
```
returns informational messages
```
## /workers/{user_email}

### GET /workers/{user_email}

```javascript
{
    userid : 'mail@email.com'
    loggedin_on : date
    status: 'loggedout/loggedin'
  }
```
Loads user info from database and returns it

## /workers/loggedin

### GET /workers/loggedin

```javascript
[
    {
        userid : 'mail@email.com'
        loggedin_on : date
        status: 'loggedout/loggedin'
    },
    ...
]
```
Returns array containing currently logged users


# /events

## GET /events?user=user&amp;type=type&amp;n=N&amp;task=task_id

```javascript
[
    {
        user: 'user@email.com',
        date: date,
        action: 'action string',
        task_id: task_id
    }
]
```

Returns not more than N events in chronological order with event type=type for user=user or for task=task_id.
If user is set than task is ignored.<br/>
Event type must be one of the following string:

* loginout
* add
* update
* peek
* suspend
* activate
* release
* complete-success
* complete-failure
* grab
* delete
* login
* logout


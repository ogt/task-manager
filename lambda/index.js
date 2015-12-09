var AWS = require("aws-sdk");

var dynamodb = new AWS.DynamoDB({
    region: "us-west-2"
});

var UserStatus = {
    IS_LOGGEDOUT: 0,
    IS_LOGGEDIN: 1
};

var TaskStatus = {
    IN_QUEUE: 0,
    FOREGROUND: 1,
    BACKGROUND: 2,
    DELETED: 3,
    DONE: 4
};

var CompletionStatus = {
    SUCCESS: 1,
    FAILURE: 0
};

validate_string = function(key, value, item) {
    if (value != undefined && value != "") {
        item[key] = {
            S: value
        };
    }

    return item;
}

validate_number = function (key, value, item) {
    if (value != undefined && value != "") {
        var result = parseInt(value);
        if (!isNaN(result)) {
            item[key] = {
                N: result.toString()
            };
        }
    }

    return item;
}

validate_date = function(key, value, item, default_value) {
    if (value != undefined && value != "") {
        var result = new Date(value).getTime();
        item = validate_number(key, result, item);
    } else if (default_value != undefined) {
        item = validate_number(key, default_value, item);
    }

    return item;
}

fail_message = function(event, context, message) {
    if (event.is_slack) {
        context.succeed({text: message});
    } else {
        context.fail(message);
    }
}

log_event = function(user_id, action, item, callback) {
    item["Date"] = {N: (new Date()).getTime().toString()};
    item["User"] = {S: user_id};
    item["Action"] = {S: action};
    dynamodb.putItem({
        TableName: "TaskManager_Events",
        Item: item
    }, function(err, data) {
        if (err) {
            console.log(err);
            console.log(item);
        }
        callback();
    });
}

format_date = function(value) {
    var result = (new Date(parseInt(value))).toISOString();

    return result;
}

format_task_item = function(task) {
    var result = {
        title: task.Title.S,
    };

    if (task.Description != undefined && task.Description.S != undefined) {
        result.description = task.Description.S;
    }

    if (task.Deadline) {
        result.deadline = format_date(parseInt(task.Deadline.N));
    }

    if (task.Priority) {
        result.priority = task.Priority.N;
    }

    if (task.Estimate) {
        result.estimate = task.Estimate.N;
    }

    if (task.Instructions) {
        result.instructions = task.Instructions.S;
    }

    result.owner = task.Owner.S;

    if (task.Properties != undefined && task.Properties.M != undefined) {
        result.properties = {};
        for (key in task.Properties.M) {
            if (task.Properties.M.hasOwnProperty(key)) {
                result.properties[key] = task.Properties.M[key].S;
            }
        }
    }

    result._id = task.TaskId.N;
    result._created_by = task.CreatedBy.S;
    result._created_on = format_date(task.CreatedOn.N);
    if (task.ModifiedOn != undefined && task.ModifiedOn.N != undefined) {
        result._last_modified_on = format_date(task.ModifiedOn.N);
    }
    var state = "";
    switch (parseInt(task.Status.N)) {
        case TaskStatus.IN_QUEUE:
            state = "queued";
            break;
        case TaskStatus.FOREGROUND:
        case TaskStatus.BACKGROUND:
            state = "worked on";
            break;
        case TaskStatus.DELETED:
            state = "deleted";
            break;
        case TaskStatus.DONE:
            state = "done";
            break;
    }
    result.state = state;

    if (task.GrabbedBy != undefined && task.GrabbedBy.S != undefined && task.Status.N != TaskStatus.IN_QUEUE &&
            task.Status.N != TaskStatus.DELETED) {
        result._worked_by = task.GrabbedBy.S;
        result._worked_on = format_date(task.GrabbedOn.N);
        if (task.Status.N == TaskStatus.FOREGROUND) {
            result._work_status = "active";
        } else if (task.Status.N == TaskStatus.BACKGROUND) {
            result._work_status = "suspended";
        } else if (task.CompletionStatus != undefined && task.CompletionStatus.N != undefined) {
            if (task.CompletionStatus.N == CompletionStatus.SUCCESS) {
                result._completion_status = "success";
            } else if (task.CompletionStatus.N == CompletionStatus.FAILURE) {
                result._completion_status = "failure";
            }
            result._completed_on = format_date(task.CompletedOn.N);
        }
    }

    if (task.Tags != undefined && task.Tags.SS.length > 0) {
        result._tags = task.Tags.SS.join(" ");
    }

    return result;
}

output_task_item = function(event, context, task) {
    result = format_task_item(task);

    if (event.is_slack) {
        context.succeed({text: "Task ID: " + result._id + "\n" +
            "Task Title: " + result.title});
    } else {
        context.succeed(result);
    }
}

compare_task_by_created_on = function(a, b) {
    if (parseInt(a.CreatedOn.N) < parseInt(b.CreatedOn.N)) {
        return -1;
    }
    if (parseInt(a.CreatedOn.N) > parseInt(b.CreatedOn.N)) {
        return 1;
    }

    return 0;
}

get = function(event, context) {
    if (event.task_id != undefined && event.task_id != "") {
        var task_id = parseInt(event.task_id);
        if (!isNaN(task_id)) {
            dynamodb.getItem({
                TableName: "TaskManager_Tasks",
                Key: {
                    TaskId: {N: event.task_id}
                }
            }, function(err, data) {
                if (err) {
                    console.log(err);
                    fail_message(event, context, "Oops... something went wrong! Try again later");
                } else {
                    if (data.Item != undefined) {
                        output_task_item(event, context, data.Item);
                    } else {
                        fail_message(event, context, "Task is not found!");
                    }
                }
            });
        } else {
            fail_message("You need to specify a task id!");
        }
    } else {
        fail_message("You need to specify a task id!");
    }
}

add_task_item = function(event, context, user_id, db_item) {
    var body = event.body;
    if (body.title != undefined && body.title != "") {
        db_item = validate_string("Title", body.title, db_item);
        db_item["Deadline"] = {N: ((new Date()).getTime() + 24*3600*1000).toString()};
        if (body.deadline != undefined && body.deadline != "") {
            var deadline = parseInt(body.deadline);
            if (!isNaN(deadline)) {
                db_item["Deadline"] = {N: deadline.toString()};
            }
        }
        db_item = validate_number("Priority", body.priority, db_item);
        if (!db_item.Priority) {
            db_item.Priority = {N: "0"};
        }
        db_item = validate_number("Estimate", body.estimate, db_item);
        db_item = validate_string("Description", body.description, db_item);
        if (body.tags != undefined && Array.isArray(body.tags)) {
            if (body.tags.length > 0) {
                if (db_item["Tags"] != undefined && db_item["Tags"].SS != undefined) {
                    for (var item_index = 0; item_index < db_item["Tags"].SS; item_index++) {
                        if (body.tags.indexOf(db_item["Tags"].SS[item_index]) < 0) {
                            body.tags.push(db_item["Tags"].SS[item_index]);
                        }
                    }
                }
                db_item["Tags"] = {
                    SS: body.tags
                };
            }
        }
        if (body.instructions != undefined && body.instructions != "") {
            db_item["Instructions"] = {S: body.instructions};
        }

        save_item = function(user_id, event, context, db_item) {
            var body = event.body;
            log_event(user_id, event.action, {TaskId: db_item.TaskId}, function() {
                dynamodb.putItem({
                    TableName: "TaskManager_Tasks",
                    Item: db_item
                }, function(err, data) {
                    console.log(db_item);
                    if (err) {
                        console.log(db_item);
                        console.log(err);
                        fail_message(event, context, "Failed to add task");
                    } else {
                        output_task_item(event, context, db_item);
                    }
                });
            });
        }

        if (body.properties != undefined) {
            db_item.Properties = {M: {}};
            for (key in body.properties) {
                if (body.properties.hasOwnProperty(key)) {
                    db_item.Properties.M[key] = {S: body.properties[key]};
                }
            }
        }

        if (body.owner != undefined && body.owner != "") {
            dynamodb.getItem({
                TableName: "TaskManager_Users",
                Key: {
                    Email: {S: body.owner}
                }
            }, function(err, data) {
                if (err) {
                    console.log(err);
                    fail_message(event, context, "Oops, looks like something is wrong... Try again later");
                } else {
                    if (data.Item) {
                        var owner = data.Item;
                        db_item.Owner = owner.Email;
                    }
                    save_item(user_id, event, context, db_item);
                }
            });
        } else {
            save_item(user_id, event, context, db_item);
        }
    } else {
        fail_message(event, context, "A title for this task is required!!");
    }
}

add = function(event, context, user_id) {
    dynamodb.updateItem({
        TableName: "TaskManager_AtomicCounters",
        Key: {
            CounterId: {S: "TaskId"}
        },
        ExpressionAttributeNames: {
            "#value": "Value"
        },
        ExpressionAttributeValues: {
            ":one": {N: "1"}
        },
        UpdateExpression: "SET #value = #value + :one",
        ReturnValues: "UPDATED_OLD"
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops... something went wrong, try again later");
        } else {
            var task_id = data.Attributes.Value;

            var db_item = {
                TaskId: task_id,
                Status: {
                    N: TaskStatus.IN_QUEUE.toString()
                },
                CreatedBy: {
                    S: user_id
                },
                CreatedOn: {
                    N: (new Date()).getTime().toString()
                },
                Owner: {
                    S: user_id
                }
            };

            add_task_item(event, context, user_id, db_item);
        }
    });
}

update = function(event, context, user_id) {
    if (event.task_id != undefined && event.task_id != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: {
                    N: event.task_id
                }
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Cannot find specified task!");
            } else {
                var task = data.Item;
                if (task != undefined) {
                    if (task.Status.N != TaskStatus.IN_QUEUE) {
                        fail_message(event, context, "Task is already being worked on!");
                    } else {
                        log_event(user_id, event.action, {
                            TaskId: task.TaskId
                        }, function() {
                            var db_item = {
                                TaskId: task.TaskId,
                                Status: task.Status,
                                CreatedBy: task.CreatedBy,
                                CreatedOn: task.CreatedOn,
                                ModifiedOn: {N: (new Date()).getTime().toString()},
                                Owner: task.Owner,
                                Tags: task.Tags
                            };

                            add_task_item(event, context, user_id, db_item);
                        });
                    }
                } else {
                    fail_message(event, context, "Task ID is not found!");
                }
            }
        });
    } else {
        fail_message(event, context, "Please provide a task ID");
    }
}

history = function(event, context) {
    if (event.task_id != undefined && event.task_id != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: {N: event.task_id}
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops, looks like there are some problems please try again later!");
            } else {
                if (data.Item) {
                    var task = data.Item;
                    dynamodb.query({
                        TableName: "TaskManager_Events",
                        ExpressionAttributeNames: {
                            "#task_id": "TaskId"
                        },
                        ExpressionAttributeValues: {
                            ":task_id": task.TaskId
                        },
                        KeyConditionExpression: "#task_id = :task_id",
                        ScanIndexForward: true,
                        IndexName: "TaskId-Date-Index"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops, looks like there are some problems, please try again later!");
                        } else {
                            var now = (new Date()).getTime();
                            var deadline = parseInt(task.Deadline.N);
                            var created = parseInt(task.CreatedOn.N);
                            var result = {
                                events: [],
                                time_elapsed: now - created,
                                time_till_deadline: deadline - now,
                                time_queued: 0,
                                time_rejected: 0,
                                times_rejected: 0
                            };
                            var grabbed_on = 0;
                            var in_queue_since = created;
                            if (data.Items.length > 0) {
                                for (var item_index = 0; item_index < data.Items.length; item_index++) {
                                    var item = data.Items[item_index];
                                    result.events.push({
                                        when: format_date(item["Date"].N),
                                        who: item.User.S,
                                        what: item.Action.S
                                    });
                                    if (item.Action.S == "grab" || item.Action.S == "release" ||
                                            item.Action.S == "done") {
                                        var action_time = parseInt(item["Date"].N);
                                        if (item.Action.S == "grab") {
                                            result.time_queued += (action_time - in_queue_since);
                                            grabbed_on = action_time;
                                        } else if (item.Action.S == "release") {
                                            result.time_rejected += (action_time - grabbed_on);
                                            result.times_rejected++;
                                            in_queue_since = action_time;
                                        }
                                    }
                                }

                                if (task.Status.N == TaskStatus.FOREGROUND) {
                                    result.time_worked_on = now - parseInt(task.GrabbedOn.N);
                                }
                                if (task.Status.N == TaskStatus.BACKGROUND) {
                                    result.time_suspended = now - parseInt(task.SuspendedOn.N);
                                }

                                if (task.Status.N == TaskStatus.IN_QUEUE) {
                                    result.time_queued += (now - in_queue_since);
                                }
                            }

                            context.succeed(result);
                        }
                    });
                } else {
                    fail_message(event, context, "Task with specified ID not found!");
                }
            }
        });
    } else {
        fail_message(event, context, "Please provide a task ID");
    }
}

worker_jobs = function(event, context) {
    if (event.user_email != undefined && event.user_email != "") {
        dynamodb.scan({
            TableName: "TaskManager_Tasks",
            ExpressionAttributeNames: {
                "#grabbed_by": "GrabbedBy"
            },
            ExpressionAttributeValues: {
                ":grabbed_by": {S: event.user_email}
            },
            FilterExpression: "#grabbed_by = :grabbed_by"
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops... looks like something is wrong! Try again later");
            } else {
                if (data.Items.length > 0) {
                    var output = [];
                    for (var task_index = 0; task_index < data.Items.length; task_index++) {
                        var task = data.Items[task_index];
                        if (task.Status.N != TaskStatus.IN_QUEUE && task.Status.N != TaskStatus.DELETE && task.Status.N != TaskStatus.DONE) {
                            output.push(format_task_item(task));
                        }
                    }

                    context.succeed(output);
                } else {
                    fail_message(event, context, "No jobs currently grabbed");
                }
            }
        });
    }
}

worker_stats = function(event, context) {
    if (event.worker != undefined && event.worker != "") {
        var KeyConditionExpression = "#user = :user";
        var ExpressionAttributeNames = {"#user": "User"};
        var ExpressionAttributeValues = {":user": {S: event.worker}};
        if (event.from != undefined && event.from != "") {
            var start = parseInt(event.from);
            if (!isNaN(start)) {
                KeyConditionExpression += " AND #date >= :start";
                ExpressionAttributeNames["#date"] = "Date";
                ExpressionAttributeValues[":start"] = {N: start.toString()};
            }
        }
        if (event.to != undefined && event.to != "") {
            var end = parseInt(event.to);
            if (!isNaN(end)) {
                KeyConditionExpression += " AND #date <= :end";
                ExpressionAttributeNames["#date"] = "Date";
                ExpressionAttributeValues[":end"] = {N: end.toString()};
            }
        }
        dynamodb.query({
            TableName: "TaskManager_Events",
            ExpressionAttributeNames: ExpressionAttributeNames,
            ExpressionAttributeValues: ExpressionAttributeValues,
            KeyConditionExpression: KeyConditionExpression,
            ScanIndexForward: true,
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops, looks like something went wrong...");
            } else {
                var session_count = 0;
                var total_session_length = 0;
                var tasks_grabbed = 0;
                var tasks_completed = 0;
                var tasks_completed_successfully = 0;
                var rejection = 0;
                var failure = 0;
                if (data.Items.length > 0) {
                    var session_start = 0;
                    for (var item_index = 0; item_index < data.Items.length; item_index++) {
                        var item = data.Items[item_index];
                        if (item.Action.S == "login") {
                            session_start = parseInt(item["Date"].N);
                            session_count++;
                        }
                        if (item.Action.S == "logout" && session_start > 0) {
                            total_session_length += (parseInt(item["Date"].N) - session_start);
                            session_start = 0;
                        }
                        if (item.Action.S == "grab") {
                            tasks_grabbed++;
                        }
                        if (item.Action.S == "complete-success") {
                            tasks_completed++;
                            tasks_completed_successfully++;
                        }
                        if (item.Action.S == "complete-failure") {
                            tasks_completed++;
                            failure++;
                        }
                        if (item.Action.S == "release") {
                            rejection++;
                        }
                    }
                }

                context.succeed({
                    "avg_session_length": (session_count > 0) ? total_session_length / session_count : 0,
                    "num_of_sessions": session_count,
                    "tasks_grabbed": tasks_grabbed,
                    "tasks_completed": tasks_completed,
                    "tasks_completed_successfully": tasks_completed_successfully,
                    "rejection_ratio": (tasks_grabbed > 0) ? rejection / tasks_grabbed : 0,
                    "failure_ratio": (tasks_completed > 0) ? failure / tasks_completed : 0,
                });
            }
        });
    } else {
        fail_message(event, context, "You need to specify a worker");
    }
}

events = function(event, context) {
    print_events = function(event, context, data, limit) {
        var result = [];

        if (data.Items.length > 0) {
            var maximum = data.Items.length;
            if (limit > 0) {
                if (limit < maximum) {
                    maximum = limit;
                }
            }
            for (var item_index = 0; item_index < maximum; item_index++) {
                var item = data.Items[item_index];
                var log_event = {
                    user: item.User.S,
                    date: format_date(parseInt(item["Date"].N)),
                    action: item.Action.S
                };
                if (item.TaskId && item.TaskId.N != "") {
                    var task_id = parseInt(item.TaskId.N);
                    if (!isNaN(task_id)) {
                        log_event.task_id = task_id;
                    }
                }
                result.push(log_event);
            }
        }

        context.succeed(result);
    }

    parse_params = function(event, context, params) {
        var hasErrors = false;
        if (event.type != "") {
            if (event.type == "loginout") {
                params.ExpressionAttributeNames["#action"] = "Action";
                params.ExpressionAttributeValues[":login"] = {S: "login"};
                params.ExpressionAttributeValues[":logout"] = {S: "logout"};
                params.FilterExpression = "#action IN (:login, :logout)";
            } else {
                switch (event.type) {
                    case "add":
                    case "update":
                    case "peek":
                    case "suspend":
                    case "activate":
                    case "release":
                    case "complete-success":
                    case "complete-failure":
                    case "grab":
                    case "delete":
                    case "login":
                    case "logout":
                        params.ExpressionAttributeNames["#action"] = "Action";
                        params.ExpressionAttributeValues[":add"] = {S: event.type};
                        params.KeyConditionExpression += " and #action = :add";
                        break;
                    default:
                        hasErrors = true;
                        fail_message(event, context, "Event type not recognized!");
                        break;
                }
            }
        }
        var limit = -1;
        if (event.n != "") {
            limit = parseInt(event.n);
            if (!isNaN(limit) && limit > 0) {
                params.Limit = limit*5;
                limit = params.Limit;
            }
        }

        return {params: params, hasError: hasErrors, limit: limit};
    }

    if (event.user != "") {
        var parse_result = parse_params(event, context, {
            TableName: "TaskManager_Events",
            ExpressionAttributeNames: {
                "#user": "User"
            },
            ExpressionAttributeValues: {
                ":user": {S: event.user}
            },
            KeyConditionExpression: "#user = :user",
            ScanIndexForward: true
        });

        if (!parse_result.hasErrors) {
            dynamodb.query(parse_result.params, function(err, data) {
                if (err) {
                    console.log(parse_result.params);
                    console.log(err);
                    fail_message(event, context, "Oops... something went wrong");
                } else {
                    print_events(event, context, data, parse_result.limit);
                }
            });
        }
    } else if (event.task != "") {
        var parse_result = parse_params(event, context, {
            TableName: "TaskManager_Events",
            ExpressionAttributeNames: {
                "#task_id": "TaskId"
            },
            ExpressionAttributeValues: {
                ":task_id": {N: event.task}
            },
            KeyConditionExpression: "#task_id = :task_id",
            ScanIndexForward: true,
            IndexName: "TaskId-Date-Index"
        });

        if (!parse_result.hasErrors) {
            dynamodb.query(parse_result.params, function(err, data) {
                if (err) {
                    console.log(err);
                    fail_message(event, context, "Oops... something went wrong");
                } else {
                    print_events(event, context, data, limit);
                }
            });
        }
    } else {
        fail_message(event, context, "Please specify a user or task to filter!");
    }
}

suspend_task = function(event, context, user_id, task, callback) {
    log_event(user_id, "suspend", {TaskId: task.TaskId}, function() {
        dynamodb.updateItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: task.TaskId
            },
            ExpressionAttributeNames: {
                "#status": "Status",
                "#suspended_by": "SuspendedBy",
                "#suspended_on": "SuspendedOn"
            },
            ExpressionAttributeValues: {
                ":status": {N: TaskStatus.BACKGROUND.toString()},
                ":suspended_by": {S: user_id},
                ":suspended_on": {N: (new Date()).getTime().toString()}
            },
            UpdateExpression: "SET #status = :status, #suspended_by = :suspended_by, #suspended_on = :suspended_on"
        }, function(err, data) {
            if (err) {
                console.log("Failed to suspend current task");
                console.log(err);
                fail_message(event, context, "Oops... something went wrong! Try again later");
            } else {
                callback(data);
            }
        });
    });
}

activate_task = function(event, context, user_id, task, callback) {
    log_event(user_id, "activate", {TaskId: task.TaskId}, function() {
        dynamodb.updateItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: task.TaskId
            },
            ExpressionAttributeNames: {
                "#status": "Status",
                "#activated_by": "ActivatedBy",
                "#activated_on": "ActivatedOn"
            },
            ExpressionAttributeValues: {
                ":status": {N: TaskStatus.FOREGROUND.toString()},
                ":activated_by": {S: user_id},
                ":activated_on": {N: (new Date()).getTime().toString()}
            },
            UpdateExpression: "SET #status = :status, #activated_by = :activated_by, #activated_on = :activated_on"
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops... something went wrong! Try again later");
            } else {
                callback(data);
            }
        });
    });
}

get_foreground_task = function(event, context, user_id, callback) {
    dynamodb.scan({
        TableName: "TaskManager_Tasks",
        ExpressionAttributeNames: {
            "#grabbed_by": "GrabbedBy",
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":grabbed_by": {S: user_id},
            ":status": {N: TaskStatus.FOREGROUND.toString()}
        },
        FilterExpression: "#grabbed_by = :grabbed_by and #status = :status"
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops, looks like something is wrong... Try again later");
        } else {
            if (data.Items.length > 0) {
                var task = data.Items[0];
                callback(task);
            } else {
                fail_message(event, context, "You have no foreground tasks!");
            }
        }
    });
}

task_status = function(event, context, user_id) {
    switch (event.status) {
        case "bg":
            if (event.task_id) {
                dynamodb.getItem({
                    TableName: "TaskManager_Tasks",
                    Key: {
                        TaskId: {N: event.task_id}
                    }
                }, function(err, data) {
                    if (err) {
                        console.log(err);
                        fail_message(event, context, "Oops.. something went wrong! Try again later");
                    } else {
                        if (data.Item) {
                            if (data.Item.Status.N != TaskStatus.FOREGROUND) {
                                fail_message(event, context, "This task is not active!");
                            } else if (data.Item.GrabbedBy && data.Item.GrabbedBy.S != user_id || !data.Item.GrabbedBy) {
                                fail_message(event, context, "This task is not your active task!");
                            } else {
                                suspend_task(event, context, user_id, data.Item, function() {
                                    context.succeed("Task suspended");
                                });
                            }
                        } else {
                            fail_message(event, context, "Task is not found!");
                        }
                    }
                });
            } else {
                get_foreground_task(event, context, user_id, function(task) {
                    suspend_task(event, context, user_id, task, function() {
                        context.succeed("Task suspended");
                    });
                });
            }
            break;
        case "fg":
            dynamodb.scan({
                TableName: "TaskManager_Tasks",
                ExpressionAttributeNames: {
                    "#grabbed_by": "GrabbedBy",
                    "#status": "Status"
                },
                ExpressionAttributeValues: {
                    ":grabbed_by": {S: user_id},
                    ":status": {N: TaskStatus.FOREGROUND.toString()}
                },
                FilterExpression: "#grabbed_by = :grabbed_by and #status = :status"
            }, function(err, data) {
                if (err) {
                    console.log(err);
                    fail_message(event, context, "Oops... something went wrong! Try again later");
                } else {
                    if (data.Items.length > 0) {
                        fail_message(event, context, "You already have a foreground task!");
                    } else {
                        if (event.task_id) {
                            dynamodb.getItem({
                                TableName: "TaskManager_Tasks",
                                Key: {
                                    TaskId: {N: event.task_id}
                                }
                            }, function(err, data) {
                                if (err) {
                                    console.log(err);
                                    fail_message(event, context, "Oops.. something went wrong! Try again later");
                                } else {
                                    if (data.Item) {
                                        if (data.Item.Status.N != TaskStatus.BACKGROUND) {
                                            fail_message(event, context, "This task is not suspended!");
                                        } else if (data.Item.GrabbedBy && data.Item.GrabbedBy.S != user_id || !data.Item.GrabbedBy) {
                                            fail_message(event, context, "This is not your suspended task!");
                                        } else {
                                            activate_task(event, context, user_id, data.Item, function() {
                                                context.succeed("Task resumed");
                                            });
                                        }
                                    } else {
                                        fail_message(event, context, "Task is not found!");
                                    }
                                }
                            });
                        } else {
                            dynamodb.scan({
                                TableName: "TaskManager_Tasks",
                                ExpressionAttributeNames: {
                                    "#status": "Status",
                                    "#grabbed_by": "GrabbedBy"
                                },
                                ExpressionAttributeValues: {
                                    ":status": {N: TaskStatus.BACKGROUND.toString()},
                                    ":grabbed_by": {S: user_id}
                                },
                                FilterExpression: "#status = :status and #grabbed_by = :grabbed_by"
                            }, function(err, data) {
                                if (err) {
                                    console.log(err);
                                    fail_message(event, context, "Oops.. something went wrong! Try again later");
                                } else {
                                    if (data.Items.length == 0) {
                                        fail_message(event, context, "You have no background jobs!");
                                    } else {
                                        var task = data.Items[0];
                                        activate_task(event, context, user_id, task, function() {
                                            context.succeed("Task activated");
                                        });
                                    }
                                }
                            });
                        }
                    }
                }
            });
            break;
        case "release":
            get_foreground_task(event, context, user_id, function(task) {
                log_event(user_id, "release", {TaskId: task.TaskId}, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: task.TaskId
                        },
                        ExpressionAttributeNames: {
                            "#status": "Status",
                            "#released_by": "ReleasedBy",
                            "#released_on": "ReleasedOn"
                        },
                        ExpressionAttributeValues: {
                            ":status": {N: TaskStatus.IN_QUEUE.toString()},
                            ":released_by": {S: user_id},
                            ":released_on": {N: (new Date()).getTime().toString()}
                        },
                        UpdateExpression: "SET #status = :status, #released_by = :released_by, #released_on = :released_on"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops, looks like something is wrong... Try again later");
                        } else {
                            context.succeed("Task released");
                        }
                    });
                });
            });
            break;
        case "complete-success":
            get_foreground_task(event, context, user_id, function(task) {
                log_event(user_id, "complete-success", {TaskId: task.TaskId}, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: task.TaskId
                        },
                        ExpressionAttributeNames: {
                            "#status": "Status",
                            "#completion_status": "CompletionStatus",
                            "#completed_by": "CompletedBy",
                            "#completed_on": "CompletedOn"
                        },
                        ExpressionAttributeValues: {
                            ":status": {N: TaskStatus.DONE.toString()},
                            ":completion_status": {N: CompletionStatus.SUCCESS.toString()},
                            ":completed_by": {S: user_id},
                            ":completed_on": {N: (new Date()).getTime().toString()}
                        },
                        UpdateExpression: "SET #status = :status, #completion_status = :completion_status, #completed_by = :completed_by, #completed_on = :completed_on"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops, looks like something is wrong... Try again later");
                        } else {
                            context.succeed("Task completed");
                        }
                    });
                });
            });
            break;
        case "complete-failure":
            get_foreground_task(event, context, user_id, function(task) {
                log_event(user_id, "complete-failure", {TaskId: task.TaskId}, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: task.TaskId
                        },
                        ExpressionAttributeNames: {
                            "#status": "Status",
                            "#completion_status": "CompletionStatus",
                            "#completed_by": "CompletedBy",
                            "#completed_on": "CompletedOn"
                        },
                        ExpressionAttributeValues: {
                            ":status": {N: TaskStatus.DONE.toString()},
                            ":completion_status": {N: CompletionStatus.FAILURE.toString()},
                            ":completed_by": {S: user_id},
                            ":completed_on": {N: (new Date()).getTime().toString()}
                        },
                        UpdateExpression: "SET #status = :status, #completion_status = :completion_status, #completed_by = :completed_by, #completed_on = :completed_on"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops, looks like something is wrong... Try again later");
                        } else {
                            context.succeed("Task completed");
                        }
                    });
                });
            });
            break;
    }
}

get_available_task = function(event, context, n, callback) {
    dynamodb.query({
        TableName: "TaskManager_Tasks",
        ExpressionAttributeNames: {
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":status": {N: TaskStatus.IN_QUEUE.toString()}
        },
        KeyConditionExpression: "#status = :status",
        IndexName: "Status-Priority-index",
        ScanIndexForward: false
    }, function(err, data) {
        if (err) {
            console.log("Failed to scan available tasks");
            console.log(err);
            fail_message(event, context, "Oops, looks like there are some problems, please try again later!");
        } else {
            if (data.Items.length > 0) {
                // @Robustness If the result exceeds 1MB within the same priority status, the
                // result might not be correct
                var tasks = [];
                var top_priority = data.Items[0].Priority.N;
                for (var task_index = 0; task_index < data.Items.length; task_index++) {
                    var task = data.Items[task_index];
                    if (task.Priority.N != top_priority) {
                        break;
                    }
                    tasks.push(task);
                }
                tasks.sort(compare_task_by_created_on);
                callback(tasks.slice(0, n));
            } else {
                context.succeed({text: "There seems to be no tasks to work on at this time! Try again later"});
            }
        }
    });
}

list_available = function(event, context) {
    var n = 1;
    if (event.n != undefined && event.n != "") {
        var n_tmp = parseInt(event.n);
        if (!isNaN(n_tmp)) {
            n = n_tmp;
        }
    }
    get_available_task(event, context, n, function(tasks) {
        var result = [];
        for (var task_index = 0; task_index < tasks.length; task_index++) {
            result.push(format_task_item(tasks[task_index]));
        }

        context.succeed(result);
    });
}

grab = function(event, context, user_id) {
    grab_task = function(task) {
        dynamodb.scan({
            TableName: "TaskManager_Tasks",
            ExpressionAttributeNames: {
                "#grabbed_by": "GrabbedBy",
                "#status": "Status"
            },
            ExpressionAttributeValues: {
                ":grabbed_by": {S: user_id},
                ":status": {N: TaskStatus.FOREGROUND.toString()}
            },
            FilterExpression: "#grabbed_by = :grabbed_by and #status = :status"
        }, function(err, active_tasks) {
            if (err) {
                console.log("Failed to scan for grabbed tasks");
                console.log(err);
                fail_message(event, context, "Oops... something went wrong! Try again later");
            } else {
                grab_task = function(user_id, task) {
                    log_event(user_id, event.action, {
                        TaskId: task.TaskId
                    }, function() {
                        var params = {
                            TableName: "TaskManager_Tasks",
                            Key: {
                                TaskId: task.TaskId
                            },
                            ExpressionAttributeNames: {
                                "#status": "Status",
                                "#grabbed_on": "GrabbedOn",
                                "#grabbed_by": "GrabbedBy"
                            },
                            ExpressionAttributeValues: {
                                ":status": {N: TaskStatus.FOREGROUND.toString()},
                                ":grabbed_on": {N: (new Date()).getTime().toString()},
                                ":grabbed_by": {S: user_id}
                            },
                            UpdateExpression: "SET #status = :status, #grabbed_on = :grabbed_on, #grabbed_by = :grabbed_by"
                        };
                        dynamodb.updateItem(params, function(err, data) {
                            if (err) {
                                console.log("Failed to grab task");
                                console.log(params);
                                console.log(err);
                                fail_message(event, context, "Oops... something went wrong! Please try again later");
                            } else {
                                task.Status = params.ExpressionAttributeValues[":status"];
                                task.GrabbedOn = params.ExpressionAttributeValues[":grabbed_on"];
                                task.GrabbedBy = params.ExpressionAttributeValues[":grabbed_by"];
                                output_task_item(event, context, task);
                            }
                        });
                    });
                }

                if (active_tasks.Items.length > 0) {
                    suspend_task(event, context, user_id, active_tasks.Items[0], function(data) {
                        grab_task(user_id, task);
                    });
                } else {
                    grab_task(user_id, task);
                }
            }
        });
    }

    if (event.task_id != undefined && event.task_id != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: {N: event.task_id}
            }
        }, function(err, data) {
            if (err) {
                console.log("Failed to get task");
                console.log(err);
                fail_message(event, context, "Oops... something went wrong! Try again later");
            } else {
                if (data) {
                    var task = data.Item;
                    if (task.Status.N != TaskStatus.IN_QUEUE.toString()) {
                        fail_message(event, context, "Task is not in queue!");
                    } else {
                        grab_task(task);
                    }
                } else {
                    fail_message(event, context, "Task is not found!");
                }
            }
        });
    } else {
        get_available_task(event, context, 1, function(tasks) {
            grab_task(tasks[0]);
        });
    }
}

get_user_item = function(item) {
    var user = {
        userid: item.Email.S
    };
    if (item.LastLoggedIn && item.LastLoggedIn.N != "") {
        user.loggedin_on = format_date(item.LastLoggedIn.N);
    }
    if (item.Status && item.Status.N != "") {
        switch (parseInt(item.Status.N)) {
            case UserStatus.IS_LOGGEDOUT:
                user.status = "loggedout";
                break;
            case UserStatus.IS_LOGGEDIN:
                user.status = "loggedin";
                break;
        }
    }

    return user;
}

who = function(event, context) {
    dynamodb.scan({
        TableName: "TaskManager_Users",
        ExpressionAttributeNames: {
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":status": {N: UserStatus.IS_LOGGEDIN.toString()}
        },
        FilterExpression: "#status = :status"
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops, looks like something went wrong... Try again later");
        } else {
            var result = [];
            for (var item_index = 0; item_index < data.Items.length; item_index++) {
                var item = data.Items[item_index];
                result.push(get_user_item(item));
            }

            context.succeed(result);
        }
    });
}

finger = function(event, context) {
    if (event.email != undefined && event.email != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Users",
            Key: {
                Email: {S: event.email}
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops, looks like something went wrong.. Try again later");
            } else {
                if (data.Item) {
                    context.succeed(get_user_item(data.Item));
                } else {
                    fail_message(event, context, "User is not found!");
                }
            }
        });
    } else {
        fail_message(event, context, "Please provide an email");
    }
}

users = function(event, context) {
    dynamodb.scan({
        TableName: "TaskManager_Users",
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops, looks like something went wrong... Try again later");
        } else {
            var result = [];
            for (var item_index = 0; item_index < data.Items.length; item_index++) {
                var item = data.Items[item_index];
                result.push(get_user_item(item));
            }

            context.succeed(result);
        }
    });
}

release = function(event, context, user_id) {
    dynamodb.scan({
        TableName: "TaskManager_Tasks",
        ExpressionAttributeNames: {
            "#grabbed_by": "GrabbedBy",
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":grabbed_by": {S: user_id},
            ":status": {N: TaskStatus.FOREGROUND.toString()}
        },
        FilterExpression: "#grabbed_by = :grabbed_by and #status = :status"
    }, function(err, data) {
        if (err) {
            console.log(err);
            context.succeed({text: "Oops, looks like something is wrong... Try again later"});
        } else {
            if (data.Items.length > 0) {
                var task = data.Items[0];
                log_event(user_id, event.action, {
                    TaskId: task.TaskId
                }, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: task.TaskId
                        },
                        ExpressionAttributeNames: {
                            "#status": "Status",
                            "#released_by": "ReleasedBy",
                            "#released_on": "ReleasedOn"
                        },
                        ExpressionAttributeValues: {
                            ":status": {N: TaskStatus.IN_QUEUE.toString()},
                            ":released_by": {S: user_id},
                            ":released_on": {N: (new Date()).getTime().toString()},
                        },
                        UpdateExpression: "SET #status = :status, #released_by = :released_by, #released_on = :released_on"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            context.succeed({text: "Oops... something went wrong! Please try again later"});
                        } else {
                            context.succeed({text: "Okay, job released"});
                        }
                    });
                });
            } else {
                context.succeed({text: "You don't seem to be working on anything now"});
            }
        }
    });
}

purge = function(event, context, user_id) {
    if (event.task_id != undefined && event.task_id != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Users",
            Key: {
                Email: {S: user_id}
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops... something went wrong! Please try again later");
            } else {
                if (data.Item && data.Item.SuperUser && data.Item.SuperUser.BOOL) {
                    dynamodb.deleteItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: {N: event.task_id}
                        }
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops... something went wrong! Please try again later");
                        } else {
                            dynamodb.query({
                                TableName: "TaskManager_Events",
                                ExpressionAttributeNames: {
                                    "#task_id": "TaskId"
                                },
                                ExpressionAttributeValues: {
                                    ":task_id": {N: event.task_id}
                                },
                                KeyConditionExpression: "#task_id = :task_id",
                                ScanIndexForward: true,
                                IndexName: "TaskId-Date-Index"
                            }, function(err, data) {
                                if (err) {
                                    console.log(err);
                                    fail_message(event, context, "Oops... something went wrong! Please try again later");
                                } else {
                                    var running_queue = 0;
                                    if (data.Items.length > 0) {
                                        for (var item_index = 0; item_index < data.Items.length; item_index++) {
                                            running_queue++;
                                            dynamodb.deleteItem({
                                                TableName: "TaskManager_Events",
                                                Key: {
                                                    User: data.Items[item_index].User,
                                                    Date: data.Items[item_index].Date
                                                }
                                            }, function(err, data) {
                                                running_queue--;
                                                if (err) {
                                                    console.log(err);
                                                }
                                                if (running_queue == 0) {
                                                    context.succeed();
                                                }
                                            });
                                        }
                                    }
                                    if (running_queue == 0) {
                                        context.succeed();
                                    }
                                }
                            });
                        }
                    });
                } else {
                    fail_message(event, context, "You do not have the privileges to do this!");
                }
            }
        });
    } else {
        fail_message(event, context, "You must specify a task ID!");
    }
}

task_delete = function(event, context, user_id) {
    if (event.task_id != undefined && event.task_id != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Tasks",
            Key: {
                TaskId: {N: event.task_id}
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Cannot find specified task!");
            } else {
                var task = data.Item;
                if (task.Status.N != TaskStatus.IN_QUEUE) {
                    fail_message(event, context, "Task is already being worked on!");
                } else {
                    log_event(user_id, event.action, {
                        TaskId: {N: event.task_id}
                    }, function() {
                        var attribute_names = {
                            "#status": "Status"
                        };
                        var attribute_values = {
                            ":status": {N: TaskStatus.DELETED.toString()}
                        };
                        var update_expression = "SET #status = :status";
                        if (event.tag != "") {
                            attribute_names["#tags"] = "Tags";
                            attribute_values[":tag"] = {SS: [event.tag]};
                            update_expression += " ADD #tags :tag";
                        }
                        dynamodb.updateItem({
                            TableName: "TaskManager_Tasks",
                            Key: {
                                TaskId: {N: event.task_id}
                            },
                            ExpressionAttributeNames: attribute_names,
                            ExpressionAttributeValues: attribute_values,
                            UpdateExpression: update_expression
                        }, function(err, data) {
                            if (err) {
                                console.log(err);
                                fail_message(event, context, "Failed to delete task, please try again later");
                            } else {
                                console.log(data);
                                context.succeed();
                            }
                        });
                    });
                }
            }
        });
    } else {
        fail_message(event, context, "Please provide a task ID");
    }
}

login = function(event, context) {
    var uuid = require("uuid");
    if (event.email != undefined && event.email != "") {
        dynamodb.getItem({
            TableName: "TaskManager_Users",
            Key: {
                Email: {
                    S: event.email
                }
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Failed to login");
            } else {
                if (data.Item) {
                    var email = data.Item.Email.S;
                    if (data.Item.SuperUser && data.Item.SuperUser.BOOL &&
                            data.Item.EffectiveUser && data.Item.EffectiveUser.S != "") {
                        email = data.Item.EffectiveUser.S;
                    }
                    log_event(email, event.action, {
                    }, function() {
                        var token = uuid.v4();

                        dynamodb.updateItem({
                            TableName: "TaskManager_Users",
                            Key: {
                                Email: {S: email}
                            },
                            ExpressionAttributeNames: {
                                "#token": "Token",
                                "#status": "Status"
                            },
                            ExpressionAttributeValues: {
                                ":token": {S: token},
                                ":last_logged_in": {N: (new Date()).getTime().toString()},
                                ":status": {N: UserStatus.IS_LOGGEDIN.toString()}
                            },
                            UpdateExpression: "SET #token = :token, LastLoggedIn = :last_logged_in, #status = :status"
                        }, function(err, data) {
                            if (err) {
                                console.log(err);
                                fail_message(event, context, "Failed to login, please try again later");
                            } else {
                                console.log(data);
                                context.succeed({token: token});
                            }
                        });
                    });
                } else {
                    fail_message(event, context, "Failed to login");
                }
            }
        });
    } else {
        fail_message(event, context, "Please provide an email");
    }
}

done = function(event, context, user_id) {
    dynamodb.scan({
        TableName: "TaskManager_Tasks",
        ExpressionAttributeNames: {
            "#grabbed_by": "GrabbedBy",
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":grabbed_by": {S: user_id},
            ":status": {N: TaskStatus.FOREGROUND.toString()}
        },
        FilterExpression: "#grabbed_by = :grabbed_by and #status = :status"
    }, function(err, data) {
        if (err) {
            console.log(err);
            context.succeed({text: "Oops, looks like something is wrong... Try again later"});
        } else {
            if (data.Items.length > 0) {
                var task = data.Items[0];
                log_event(user_id, event.action, {
                    TaskId: task.TaskId
                }, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Tasks",
                        Key: {
                            TaskId: task.TaskId
                        },
                        ExpressionAttributeNames: {
                            "#status": "Status",
                            "#completed_by": "CompletedBy",
                            "#completed_on": "CompletedOn"
                        },
                        ExpressionAttributeValues: {
                            ":status": {N: TaskStatus.DONE.toString()},
                            ":completed_by": {S: user_id},
                            ":completed_on": {N: (new Date()).getTime().toString()}
                        },
                        UpdateExpression: "SET #status = :status, #completed_by = :completed_by, #completed_on = :completed_on"
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            context.succeed({text: "Oops... something went wrong! Please try again later"});
                        } else {
                            context.succeed({text: "Okay, done!"});
                        }
                    });
                });
            } else {
                context.succeed({text: "You don't seem to be doing any job right now!"});
            }
        }
    });
}

logout = function(event, context, user_id) {
    dynamodb.updateItem({
        TableName: "TaskManager_Users",
        Key: {
            Email: {S: user_id}
        },
        ExpressionAttributeNames: {
            "#token": "Token",
            "#status": "Status"
        },
        ExpressionAttributeValues: {
            ":status": {N: UserStatus.IS_LOGGEDOUT.toString()}
        },
        UpdateExpression: "SET #status = :status REMOVE #token"
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops... something failed, please try again later");
        } else {
            log_event(user_id, event.action, {}, function() {
                context.succeed();
            });
        }
    });
}

sudo = function(event, context, user_id) {
    // @Cleanup: We obviously will have already retrieved the user item, so just pass it around
    // until we get it here
    dynamodb.getItem({
        TableName: "TaskManager_Users",
        Key: {
            Email: {S: event.current_user}
        }
    }, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Oops... something went wrong!");
        } else {
            var user = data.Item;
            if (user && user.SuperUser && user.SuperUser.BOOL) {
                log_event(user_id, "sudo", {"TargetUser": event.target_user}, function() {
                    dynamodb.updateItem({
                        TableName: "TaskManager_Users",
                        Key: {
                            Email: {S: event.current_user}
                        },
                        ExpressionAttributeNames: {
                            "#effective_user": "EffectiveUser"
                        },
                        ExpressionAttributeValues: {
                            ":effective_user": {S: event.target_user}
                        },
                        UpdateExpression: "SET #effective_user = :effective_user",
                    }, function(err, data) {
                        if (err) {
                            console.log(err);
                            fail_message(event, context, "Oops, something went wrong!");
                        } else {
                            context.succeed(true);
                        }
                    });
                });
            } else {
                fail_message(event, context, "You don't have the permission to do this!");
            }
        }
    });
}

get_user_id = function(event, context, callback) {
    if (event.current_user) {
        dynamodb.getItem({
            TableName: "TaskManager_Users",
            Key: {
                Email: {S: event.current_user}
            }
        }, function(err, data) {
            if (err) {
                console.log(err);
                fail_message(event, context, "Oops... something went wrong!");
            } else {
                var user = event.current_user;
                if (data.Item) {
                    if (data.Item.EffectiveUser != undefined && data.Item.EffectiveUser.S != "") {
                        user = data.Item.EffectiveUser.S;
                    }
                }
                callback(user);
            }
        });
    } else {
        callback("");
    }
}

list = function(event, context) {
    var attribute_names = {
        "#status": "Status"
    };
    var attribute_values = {
    };
    var filter_expression = [];
    if (event.status != undefined && event.status != "") {
        var status = TaskStatus.IN_QUEUE;
        switch (event.status.toLowerCase()) {
            case "queued":
                status = TaskStatus.IN_QUEUE;
                break;
            case "active":
                status = TaskStatus.FOREGROUND;
                break;
            case "suspended":
                status = TaskStatus.BACKGROUND;
                break;
            case "completed":
                status = TaskStatus.DONE;
                break;
            case "deleted":
                status = TaskStatus.DELETED;
                break;
        }
        attribute_values[":status"] = {N: status.toString()};
    } else {
        attribute_values[":status"] = {N: TaskStatus.IN_QUEUE.toString()};
    }
    if (event.owner != undefined && event.owner != "") {
        attribute_names["#owner"] = "Owner";
        attribute_values[":owner"] = {S: event.owner};
        filter_expression.push("#owner = :owner");
    }
    if (event.tag != undefined && event.tag != "") {
        attribute_names["#tag"] = "Tags";
        attribute_values[":tag"] = {S: event.tag};
        filter_expression.push("contains(#tag, :tag)");
    }
    var params = {
        TableName: "TaskManager_Tasks",
        ExpressionAttributeNames: attribute_names,
        ExpressionAttributeValues: attribute_values,
        IndexName: "Status-Priority-index",
        KeyConditionExpression: "#status = :status",
        ScanIndexForward: false,
    };
    if (filter_expression.length > 0) {
        params.FilterExpression = filter_expression.join(" and ");
    }
    console.log(params);
    dynamodb.query(params, function(err, data) {
        if (err) {
            console.log(err);
            fail_message(event, context, "Failed to list tasks, please try again later");
        } else {
            var result = [];
            console.log(data);
            if (data.Items.length > 0) {
                var maximum = data.Items.length;
                if (event.n != undefined && event.n != "") {
                    var n = parseInt(event.n);
                    if (!isNaN(n)) {
                        if (n < maximum) {
                            maximum = n;
                        }
                    }
                } else if (maximum > 100) {
                    maximum = 100;
                }

                var tasks = {};
                var priority_index = 0;
                var prev_priority = -100;
                for (var task_index = 0; task_index < data.Items.length; task_index++) {
                    var task = data.Items[task_index];
                    if (task.priority != prev_priority) {
                        priority_index++;
                        prev_priority = task.priority;
                        if (tasks[priority_index.toString()] == undefined) {
                            tasks[priority_index.toString()] = [];
                        }
                    }
                    tasks[priority_index.toString()].push(task);
                }

                for (var priority in tasks) {
                    if (tasks.hasOwnProperty(priority)) {
                        tasks[priority].sort(compare_task_by_created_on);
                        for (var task_index = 0; task_index < tasks[priority].length; task_index++) {
                            result.push(format_task_item(tasks[priority][task_index]));
                            if (result.length >= maximum) {
                                break;
                            }
                        }
                    }
                }
            }

            context.succeed(result);
        }
    });
}

handle_nonloggedin = function(event, context) {
    switch (event.action) {
        case "login":
            login(event, context);
            break;
        case "events":
            events(event, context);
            break;
        case "who":
            who(event, context);
            break;
        case "users":
            users(event, context);
            break;
        case "get":
            get(event, context);
            break;
        case "whoami":
            context.succeed({text: "You are currently not logged in!"});
            break;
        case "list":
            list(event, context);
            break;
        case "list-available":
            list_available(event, context);
            break;
        case "history":
            history(event, context);
            break;
        case "worker-stats":
            worker_stats(event, context);
            break;
        case "worker-jobs":
            worker_jobs(event, context);
            break;
        default:
            fail_message(event, context, "Action not found, maybe you're not logged in?");
            break;
    }
}

usage = function(context) {
    context.succeed({text: "Usage: /tt [command]\nPlease consult the manual for more usage"});
}

exports.handler = function(event, context) {
    console.log(event);
    if (event.is_slack) {
        if (event.text != undefined && event.text != "") {
            var args = event.text.split(" ");
            event.action = args[0];
        } else {
            usage(context);
        }
    }
    if (event.current_user || event.user_id) {
        get_user_id(event, context, function(user_id) {
            console.log(event);
            if (!user_id) {
                delete event.token;
                handle_nonloggedin(event, context);
            } else {
                switch (event.action) {
                    case "add":
                        add(event, context, user_id);
                        break;
                    case "finger":
                        finger(event, context, user_id);
                        break;
                    case "task-status":
                        task_status(event, context, user_id);
                        break;
                    case "update":
                        update(event, context, user_id);
                        break;
                    case "delete":
                        task_delete(event, context, user_id);
                        break;
                    case "logout":
                        logout(event, context, user_id);
                        break;
                    case "whoami":
                        context.succeed({text: "You are logged in as " + user_id});
                        break;
                    case "grab":
                        grab(event, context, user_id);
                        break;
                    case "purge":
                        purge(event, context, user_id);
                        break;
                    case "sudo":
                        sudo(event, context, user_id);
                        break;
                    default:
                        fail_message(event, context, "Action not found, maybe you're trying to login again?");
                        break;
                }
            }
        });
    } else {
        handle_nonloggedin(event, context);
    }
}


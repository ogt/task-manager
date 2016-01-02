var pg = require("pg");
var raven = require("raven");
var sentry = new raven.Client('');

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

fail_message = function(client, event, context, message) {
    client.end();
    context.fail(message);
}

log_event = function(client, user_id, action, task_id, callback, target_user_id) {
    var insert = [user_id, (new Date()).getTime(), action];
    if (task_id === true) {
        client.query("INSERT INTO events (user_id, event_date, event_action, event_task_id) SELECT $1, $2, $3, (SELECT last_value + 1 FROM tasks_task_id_seq)", insert, function(err, result) {
            if (err) {
                console.log(err);
            }
            callback();
        });
    } else if (task_id) {
        insert[3] = task_id;
        client.query("INSERT INTO events (user_id, event_date, event_action, event_task_id) VALUES ($1, $2, $3, $4)", insert, function(err, result) {
            if (err) {
                console.log(err);
            }
            callback();
        });
    } else if (target_user_id) {
        insert[3] = target_user_id;
        client.query("INSERT INTO events (user_id, event_date, event_action, target_user) SELECT $1, $2, $3, (SELECT user_id FROM users WHERE user_email = $4)", insert, function(err, result) {
            if (err) {
                console.log(err);
            }
            callback();
        });
    } else {
        client.query("INSERT INTO events (user_id, event_date, event_action) VALUES ($1, $2, $3)", insert, function(err, result) {
            if (err) {
                console.log(err);
            }
            callback();
        });
    }
}

get_user_item = function(item) {
    var user = {
        userid: item.user_email
    };
    if (item.last_logged_in) {
        user.loggedin_on = format_date(item.last_logged_in);
    }
    switch (item.user_status) {
        case UserStatus.IS_LOGGEDOUT:
            user.status = "loggedout";
            break;
        case UserStatus.IS_LOGGEDIN:
            user.status = "loggedin";
            break;
    }

    return user;
}

format_date = function(value) {
    var result = (new Date(parseInt(value))).toISOString();

    return result;
}

format_task_item = function(task) {
    var result = {
        title: task.task_title,
    };

    if (task.task_description) {
        result.description = task.task_description;
    }

    if (task.task_deadline) {
        result.deadline = format_date(task.task_deadline);
    }

    if (task.priority != null) {
        result.priority = task.priority;
    }

    if (task.estimate) {
        result.estimate = task.estimate;
    }

    if (task.instructions) {
        result.instructions = task.instructions;
    }

    result.owner = task.owner_user;

    /*
    if (task.Properties != undefined && task.Properties.M != undefined) {
        result.properties = {};
        for (key in task.Properties.M) {
            if (task.Properties.M.hasOwnProperty(key)) {
                result.properties[key] = task.Properties.M[key].S;
            }
        }
    }
    */

    result._id = task.task_id;
    result._created_by = task.created_user;
    result._created_on = format_date(task.created_on);
    if (task.modified_on) {
        result._last_modified_on = format_date(task.modified_on);
    }
    var state = "";
    switch (task.task_status) {
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

    if (task.depends_on) {
        result._depends_on = task.depends_on;
    }

    if (task.grabbed_by && task.task_status != TaskStatus.IN_QUEUE &&
            task.task_status != TaskStatus.DELETED) {
        result._worked_by = task.grabbed_user;
        result._worked_on = format_date(task.grabbed_on);
        if (task.task_status == TaskStatus.FOREGROUND) {
            result._work_status = "active";
        } else if (task.task_status == TaskStatus.BACKGROUND) {
            result._work_status = "suspended";
        } else if (task.completion_status != null) {
            if (task.completion_status == CompletionStatus.SUCCESS) {
                result._completion_status = "success";
            } else if (task.completion_status) {
                result._completion_status = "failure";
            }
            result._completed_on = format_date(task.completed_on);
        }
    }

    if (task.tags && task.tags.length > 0) {
        result._tags = task.tags.join(" ");
    }

    return result;
}

output_task_item = function(client, event, context, task) {
    result = format_task_item(task);

    client.end();
    context.succeed(result);
}

get_task_by_id = function(client, event, context, task_id, callback) {
    client.query("SELECT * FROM tasks_view WHERE task_id = $1", [task_id], function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong! Try again later");
            });
        } else {
            if (data.rows.length > 0) {
                callback(data.rows[0]);
            } else {
                fail_message(client, event, context, "Cannot find task " + task_id + "!");
            }
        }
    });
};

get = function(client, event, context) {
    if (event.task_id != undefined && event.task_id != "") {
        var task_id = parseInt(event.task_id);
        if (!isNaN(task_id)) {
            get_task_by_id(client, event, context, task_id, function(task) {
                if (task) {
                    output_task_item(client, event, context, task);
                } else {
                    fail_message(client, event, context, "Task is not found!");
                }
            });
        } else {
            fail_message(client, "You need to specify a task id!");
        }
    } else {
        fail_message(client, "You need to specify a task id!");
    }
}

add_type = function(client, event, context, user_id, user_data) {
    var body = event.body;
    var query = "INSERT INTO task_types (type_name, task_title_prefix, task_title, task_description, task_deadline, priority, estimate, instructions, tags, type_created_by, type_created_on, task_owner, parent_type) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, (SELECT user_id FROM users WHERE user_email = $12), $13";

    var parameters = [null, null, null, null, null, null, null, null, null, null, null, null, null];

    parameters[9] = user_id;
    parameters[10] = (new Date()).getTime();

    if (body.name != undefined && body.name != "") {
        parameters[0] = body.name;

        if (body.title_prefix) {
            parameters[1] = body.title_prefix;
        }

        if (body.title) {
            parameters[2] = body.title;
        }

        if (body.description) {
            parameters[3] = body.description;
        }

        if (body.deadline) {
            parameters[4] = body.deadline;
        }

        if (body.priority != null) {
            parameters[5] = body.priority;
        }

        if (body.estimate) {
            parameters[6] = body.estimate;
        }

        if (body.instructions) {
            parameters[7] = body.instructions;
        }

        if (body.tags) {
            if (Array.isArray(body.tags) && body.tags.length > 0) {
                parameters[8] = '{"' + body.tags.join('","') + '"}';
            } else {
                parameters[8] = '{}';
            }
        }

        if (body.owner) {
            parameters[11] = body.owner;
        }

        if (body.parent) {
            parameters[12] = body.parent;
        }

        client.query(query, parameters, function(err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                client.end();
                context.succeed("Successfully added task type!");
            }
        });
    }
}

add = function(client, event, context, user_id, user_data) {
    if (event.body.tasks.length > 0) {
        var results = null;
        if (event.body.tasks.length > 1) {
            results = [];
        }
        var task_count = event.body.tasks.length;

        var task_ids = [];

        add_task = function(task_index, parameters, tries) {
            if (parameters[20] < 0) {
                if (task_ids[task_index + parseInt(parameters[20])]) {
                    parameters[20] = task_ids[task_index + parseInt(parameters[20])];
                } else {
                    if (!tries) {
                        tries = 0;
                    }
                    if (tries < 3) {
                        setTimeout(function() {
                            add_task(task_index, parameters, tries + 1);
                        }, 500);
                        return;
                    } else {
                        results.push({"error": "Cannot find dependent task", "index": task_index});
                        return;
                    }
                }
            } else if (!parameters[20]) {
                parameters[20] = null;
            }
            log_event(client, user_id, 'add', true, function() {
                client.query(query, parameters, function(err, data) {
                    if (err) {
                        console.log(err);
                        sentry.captureError(err, function(result) {
                            if (results) {
                                results.push({"error": "Failed to add task", "index": task_index});
                            } else {
                                fail_message(client, event, context, "Oops... something went wrong!");
                            }
                        });
                    } else {
                        task_ids[task_index] = data.rows[0].task_id;
                        get_task_by_id(client, event, context, data.rows[0].task_id, function(task) {
                            if (results != null) {
                                results.push(format_task_item(task));
                                if (results.length == task_count) {
                                    client.end();
                                    context.succeed(results);
                                }
                            } else {
                                output_task_item(client, event, context, task);
                            }
                        });
                    }
                });
            });
        };

        var base_created_on = (new Date()).getTime();

        for (var task_index = 0; task_index < event.body.tasks.length; task_index++) {
            var body = event.body.tasks[task_index];
            var query = "INSERT INTO tasks (task_title, task_description, task_deadline, task_status, priority, estimate, instructions, tags, created_by, created_on, task_owner, completed_by, completed_on, completion_status, grabbed_by, grabbed_on, released_by, released_on, suspended_by, suspended_on, depends_on, task_type) SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, (SELECT user_id FROM users WHERE user_email = $11), (SELECT user_id FROM users WHERE user_email = $12), $13, $14, (SELECT user_id FROM users WHERE user_email = $15), $16, (SELECT user_id FROM users WHERE user_email = $17), $18, (SELECT user_id FROM users WHERE user_email = $19), $20, $21, $22 RETURNING task_id";

            var parameters = [null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null];

            parameters[3] = TaskStatus.IN_QUEUE;
            parameters[8] = user_id;
            parameters[9] = base_created_on + task_index * 10;
            parameters[10] = user_data.user_email;

            if (body.title != undefined && body.title != "") {
                parameters[0] = body.title;
                parameters[2] = ((new Date()).getTime() + 24*3600*1000);
                if (body.deadline != undefined && body.deadline != "") {
                    var deadline = parseInt(body.deadline);
                    if (!isNaN(deadline)) {
                        parameters[2] = deadline;
                    }
                }
                parameters[4] = body.priority;
                parameters[5] = body.estimate;
                parameters[1] = body.description;
                if (body.tags != undefined && Array.isArray(body.tags)) {
                    if (body.tags.length > 0) {
                        parameters[7] = '{"' + body.tags.join('","') + '"}';
                    } else {
                        parameters[7] = '{}';
                    }
                }
                if (body.instructions != undefined && body.instructions != "") {
                    parameters[6] = body.instructions;
                }

                if (body.owner != undefined && body.owner != "") {
                    parameters[10] = body.owner;
                }

                if (body.depends) {
                    parameters[20] = parseInt(body.depends);
                    if (!parameters[20]) {
                        parameters[20] = null;
                    }
                }
                
                if (body.type) {
                    parameters[21] = body.type;
                }

                add_task(task_index, parameters);
            } else {
                if (results) {
                    results.push({"error": "Title required", "index": task_index});
                    if (results.length == task_count) {
                        client.end();
                        context.succeed(results);
                    }
                } else {
                    fail_message(client, event, context, "A title for this task is required!!");
                }
            }
        }

    } else {
        context.fail("Please enter a task to insert");
    }
}

update_type = function(client, event, context, user_id) {
    var body = event.body;

    if (body.type_id) {
        var updates = [];
        var parameters = [];
        var subqueries = [];

        if (body.name) {
            parameters.push(body.name);
            updates.push("type_name = $" + parameters.length);
        }

        if (body.title) {
            parameters.push(body.title);
            updates.push("task_title = $" + parameters.length);
        }

        if (body.title_prefix) {
            parameters.push(body.title_prefix);
            updates.push("task_title_prefix = $" + parameters.length);
        }

        if (body.description) {
            parameters.push(body.description);
            updates.push("task_description = $" + parameters.length);
        }

        if (body.deadline) {
            parameters.push(body.deadline);
            updates.push("task_deadline = $" + parameters.length);
        }

        if (body.priority != null) {
            parameters.push(body.priority);
            updates.push("priority = $" + parameters.length);
        }

        if (body.estimate) {
            parameters.push(body.estimate);
            updates.push("estimate = $" + parameters.length);
        }

        if (body.instructions) {
            parameters.push(body.instructions);
            updates.push("instructions = $" + parameters.length);
        }

        if (body.tags && Array.isArray(body.tags) && body.tags.length > 0) {
            var tag_update = [];
            for (var tag_index = 0; tag_index < body.tags.length; tag_index++) {
                parameters.push(body.tags[tag_index]);
                tag_update.push("$" + parameters.length);
            }
            updates.push("tags = array_cat(tags, ARRAY[" + tag_update.join(", ") + "])");
        }

        if (body.owner) {
            parameters.push(body.owner);
            updates.push("task_owner = sub.user_id");
            subqueries.push("SELECT user_id FROM users WHERE user_email = $" + parameters.length);
        }

        if (body.parent) {
            parameters.push(body.parent);
            updates.push("parent_type = $" + parameters.length);
        }

        var query = "UPDATE task_types SET " + updates.join(", ");
        if (subqueries.length > 0) {
            query += " FROM (SELECT (" + subqueries.join("), (") + ")) AS sub";
        }
        parameters.push(body.type_id);
        query += " WHERE task_types.type_id = $" + parameters.length;
        client.query(query, parameters, function(err, data) {
            if (err) {
                console.log("Failed to update type");
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                client.end();
                context.succeed("Successfully updated type");
            }
        });
    } else {
        fail_message(client, event, context, "Please specify a type ID");
    }
}

update = function(client, event, context, user_id) {
    if (event.body.tasks.length > 0) {
        var results = null;
        var task_count = event.body.tasks.length;
        if (event.body.tasks.length > 1) {
            results = [];
        }

        for (var task_index = 0; task_index < event.body.tasks.length; task_index++) {
            var body = event.body.tasks[task_index];

            if (body.task_id != undefined && body.task_id != "") {
                (function(task_index, body) {
                    client.query("SELECT * FROM tasks_view WHERE task_id = $1", [body.task_id], function(err, data) {
                        if (err) {
                            console.log(err);
                            if (results) {
                                results.push({"error": "Cannot find specified task!", "index": task_index});
                                if (results.length == task_count) {
                                    client.end();
                                    context.succeed(results);
                                }
                            } else {
                                fail_message(client, event, context, "Cannot find specified task!");
                            }
                        } else {
                            var task = data.rows[0];
                            if (task != undefined) {
                                if (task.task_status != TaskStatus.IN_QUEUE) {
                                    if (results) {
                                        results.push({"error": "Task is already being worked on!", "index": task_index});
                                        if (results.length == task_count) {
                                            client.end();
                                            context.succeed(results);
                                        }
                                    } else {
                                        fail_message(client, event, context, "Task is already being worked on!");
                                    }
                                } else {
                                    log_event(client, user_id, "update", task.task_id, function() {
                                        var updates = [];
                                        var parameters = [];
                                        var subqueries = [];
                                        parameters.push((new Date()).getTime());
                                        updates.push("modified_on = $" + parameters.length);
                                        parameters.push(user_id);
                                        updates.push("modified_by = $" + parameters.length);
                                        if (body.title) {
                                            parameters.push(body.title);
                                            updates.push("task_title = $" + parameters.length);
                                        }
                                        if (body.deadline) {
                                            var deadline = parseInt(body.deadline);
                                            if (!isNaN(deadline)) {
                                                parameters.push(deadline);
                                                updates.push("task_deadline = $" + parameters.length);
                                            }
                                        }
                                        if (body.priority) {
                                            var priority = parseInt(body.priority);
                                            if (!isNaN(priority)) {
                                                parameters.push(priority);
                                                updates.push("priority = $" + parameters.length);
                                            }
                                        }
                                        if (body.estimate) {
                                            var estimate = parseInt(body.estimate);
                                            if (!isNaN(estimate)) {
                                                parameters.push(estimate);
                                                updates.push("estimate = $" + parameters.length);
                                            }
                                        }
                                        if (body.description) {
                                            parameters.push(body.description);
                                            updates.push("task_description = $" + parameters.length);
                                        }
                                        if (body.instructions) {
                                            parameters.push(body.instructions);
                                            updates.push("instructions = $" + parameters.length);
                                        }
                                        if (body.owner) {
                                            parameters.push(body.owner);
                                            updates.push("task_owner = sub.user_id");
                                            subqueries.push("SELECT user_id FROM users WHERE user_email = $" + parameters.length);
                                        }
                                        if (body.tags && Array.isArray(body.tags) && body.tags.length > 0) {
                                            var tag_update = [];
                                            for (var tag_index = 0; tag_index < body.tags.length; tag_index++) {
                                                parameters.push(body.tags[tag_index]);
                                                tag_update.push("$" + parameters.length);
                                            }
                                            updates.push("tags = array_cat(tags, ARRAY[" + tag_update.join(", ") + "])");
                                        }
                                        if (body.depends) {
                                            parameters.push(body.depends);
                                            updates.push("depends_on = $" + parameters.length);
                                        }

                                        if (updates.length > 2) {
                                            var query = "UPDATE tasks SET " + updates.join(", ");
                                            if (subqueries.length > 0) {
                                                query += " FROM (SELECT (" + subqueries.join("), (") + ")) AS sub";
                                            }
                                            parameters.push(task.task_id);
                                            query += " WHERE tasks.task_id = $" + parameters.length;
                                            client.query(query, parameters, function(err, data) {
                                                if (err) {
                                                    sentry.captureError(err, function(result) {
                                                        if (results) {
                                                            results.push({"error": "Failed to update task", "index": task_index});
                                                        } else {
                                                            fail_message(client, event, context, "Oops... something went wrong!");
                                                        }
                                                    });
                                                } else {
                                                    get_task_by_id(client, event, context, task.task_id, function(task) {
                                                        if (results != null) {
                                                            results.push(format_task_item(task));
                                                            if (results.length == task_count) {
                                                                client.end();
                                                                context.succeed(results);
                                                            }
                                                        } else {
                                                            output_task_item(client, event, context, task);
                                                        }
                                                    });
                                                }
                                            });
                                        } else {
                                            if (results) {
                                                results.push({"error": "Nothing to update!", "index": task_index});
                                                if (results.length == task_count) {
                                                    client.end();
                                                    context.succeed(results);
                                                }
                                            } else {
                                                fail_message(client, event, context, "Nothing to update!");
                                            }
                                        }
                                    });
                                }
                            } else {
                                if (results) {
                                    results.push({"error": "Task ID " + body.task_id + " is not found!", "index": task_index});
                                    if (results.length == task_count) {
                                        client.end();
                                        context.succeed(results);
                                    }
                                } else {
                                    fail_message(client, event, context, "Task ID " + body.task_id + " is not found!");
                                }
                            }
                        }
                    });
                })(task_index, body);
            } else {
                if (results) {
                    results.push({"error": "Please provide a task ID", "index": task_index});
                    if (results.length == task_count) {
                        client.end();
                        context.succeed(results);
                    }
                } else {
                    fail_message(client, event, context, "Please provide a task ID");
                }
            }
        }
    } else {
        fail_message(client, event, context, "Nothing to update!");
    }
}

history = function(client, event, context) {
    if (event.task_id != undefined && event.task_id != "") {
        get_task_by_id(client, event, context, event.task_id, function(task) {
            if (task) {
                client.query("SELECT * FROM events e JOIN users u ON (u.user_id = e.user_id) WHERE event_task_id = " + task.task_id + " ORDER BY event_date ASC", [], function(err, data) {
                    if (err) {
                        console.log(err);
                        sentry.captureError(err, function(result) {
                            fail_message(client, event, context, "Oops... something went wrong!");
                        });
                    } else {
                        var now = (new Date()).getTime();
                        var deadline = task.task_deadline;
                        var created = task.created_on;
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
                        if (data.rows.length > 0) {
                            for (var item_index = 0; item_index < data.rows.length; item_index++) {
                                var item = data.rows[item_index];
                                result.events.push({
                                    when: format_date(item.event_date),
                                    who: item.user_email,
                                    what: item.event_action
                                });
                                if (item.event_action == "grab" || item.event_action == "release" ||
                                        item.event_action == "done") {
                                    var action_time = parseInt(item.event_date);
                                    if (item.event_action == "grab") {
                                        result.time_queued += (action_time - in_queue_since);
                                        grabbed_on = action_time;
                                    } else if (item.event_action == "release") {
                                        result.time_rejected += (action_time - grabbed_on);
                                        result.times_rejected++;
                                        in_queue_since = action_time;
                                    }
                                }
                            }

                            if (task.task_status == TaskStatus.FOREGROUND) {
                                result.time_worked_on = now - parseInt(task.grabbed_on);
                            }
                            if (task.task_status == TaskStatus.BACKGROUND) {
                                result.time_suspended = now - parseInt(task.suspended_on);
                            }

                            if (task.task_status == TaskStatus.IN_QUEUE) {
                                result.time_queued += (now - in_queue_since);
                            }
                        }

                        client.end();
                        context.succeed(result);
                    }
                });
            } else {
                fail_message(client, event, context, "Task with specified ID not found!");
            }
        });
    } else {
        fail_message(client, event, context, "Please provide a task ID");
    }
}

worker_jobs = function(client, event, context) {
    if (event.user_email != undefined && event.user_email != "") {
        client.query("SELECT * FROM tasks_view WHERE grabbed_user = $1 AND task_status IN (" + TaskStatus.FOREGROUND + ", " + TaskStatus.BACKGROUND + ") ORDER BY task_status ASC, priority DESC, created_on ASC", [event.user_email], function(err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                if (data.rows.length > 0) {
                    var output = [];
                    for (var task_index = 0; task_index < data.rows.length; task_index++) {
                        var task = data.rows[task_index];
                        output.push(format_task_item(task));
                    }

                    client.end();
                    context.succeed(output);
                } else {
                    fail_message(client, event, context, "No jobs currently grabbed");
                }
            }
        });
    } else {
        fail_message(client, event, context, "You must specify a worker!");
    }
}

worker_stats = function(client, event, context) {
    if (event.worker != undefined && event.worker != "") {
        var where_condition = ["u.user_email = $1"];
        var query_parameters = [event.worker];
        if (event.from != undefined && event.from != "") {
            var start = parseInt(event.from);
            if (!isNaN(start)) {
                query_parameters.push(start);
                where_condition.push("event_date >= $" + query_parameters.length);
            }
        }
        if (event.to != undefined && event.to != "") {
            var end = parseInt(event.to);
            if (!isNaN(end)) {
                query_parameters.push(end);
                where_condition.push("event_date < $" + query_parameters.length);
            }
        }
        var query = "SELECT * FROM events e JOIN users u ON (u.user_id = e.user_id) WHERE (" + where_condition.join(") AND (") + ") ORDER BY event_date ASC";
        client.query(query, query_parameters, function(err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                var session_count = 0;
                var total_session_length = 0;
                var tasks_grabbed = 0;
                var tasks_completed = 0;
                var tasks_completed_successfully = 0;
                var rejection = 0;
                var failure = 0;
                if (data.rows.length > 0) {
                    var session_start = 0;
                    for (var item_index = 0; item_index < data.rows.length; item_index++) {
                        var item = data.rows[item_index];
                        if (item.event_action == "login") {
                            session_start = item.event_date;
                            session_count++;
                        }
                        if (item.event_action == "logout" && session_start > 0) {
                            total_session_length += (item.event_date - session_start);
                            session_start = 0;
                        }
                        if (item.event_action == "grab") {
                            tasks_grabbed++;
                        }
                        if (item.event_action == "complete-success") {
                            tasks_completed++;
                            tasks_completed_successfully++;
                        }
                        if (item.event_action == "complete-failure") {
                            tasks_completed++;
                            failure++;
                        }
                        if (item.event_action == "release") {
                            rejection++;
                        }
                    }

                    if (session_start != 0) {
                        total_session_length += ((new Date()).getTime()) - session_start;
                    }
                }

                client.end();
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
        fail_message(client, event, context, "You need to specify a worker");
    }
}

events = function(client, event, context) {
    print_events = function(event, context, data) {
        var result = [];

        if (data.rows.length > 0) {
            for (var item_index = 0; item_index < data.rows.length; item_index++) {
                var item = data.rows[item_index];
                var log_event = {
                    user: item.user_email,
                    date: format_date(item.event_date),
                    action: item.event_action
                };
                if (item.event_task_id) {
                    log_event.task_id = item.event_task_id;
                }
                result.push(log_event);
            }
        }

        client.end();
        context.succeed(result);
    }

    var where_condition = [];
    var query_params = [];
    if (event.type) {
        if (event.type == "loginout") {
            where_condition.push("event_action IN ('login', 'logout')");
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
                    query_params.push(event.type);
                    where_condition.push("event_action = $" + query_params.length);
                    break;
                default:
                    hasErrors = true;
                    fail_message(client, event, context, "Event type not recognized!");
                    break;
            }
        }
    }

    var limit = "";
    if (event.n) {
        limit_amount = parseInt(event.n);
        if (!isNaN(limit_amount) && limit_amount > 0) {
            limit = "LIMIT " + limit_amount;
        }
    }

    if (event.user) {
        query_params.push(event.user);
        where_condition.push("u.user_email = $" + query_params.length);
    }
    if (event.task) {
        query_params.push(event.task);
        where_condition.push("event_task_id = $" + query_params.length);
    }

    var query = "SELECT * FROM events e JOIN users u ON (e.user_id = u.user_id)";
    if (where_condition.length > 0) {
        query += " WHERE (" + where_condition.join(") AND (") + ")";
    }
    if (limit) {
        query += " " + limit;
    }
    client.query(query, query_params, function(err, result) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong!");
            });
        } else {
            print_events(event, context, result);
        }
    });
}

suspend_task = function(client, event, context, user_id, task, callback) {
    var update = [user_id, (new Date()).getTime()];
    log_event(client, user_id, "suspend", task.task_id, function() {
        client.query("UPDATE tasks SET task_status = " + TaskStatus.BACKGROUND + ", suspended_by = $1, suspended_on = $2 WHERE task_id = " + task.task_id, update, function(err, data) {
            if (err) {
                console.log("Failed to suspend current task");
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong! Try again later");
                });
            } else {
                get_task_by_id(client, event, context, task.task_id, callback);
            }
        });
    });
}

activate_task = function(client, event, context, user_id, task, callback) {
    log_event(client, user_id, "activate", task.task_id, function() {
        client.query("UPDATE tasks SET task_status = $1 WHERE task_id = $2", [TaskStatus.FOREGROUND, task.task_id], function(err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong! Try again later");
                });
            } else {
                get_task_by_id(client, event, context, task.task_id, callback);
            }
        });
    });
}

get_foreground_task = function(client, event, context, user_id, callback) {
    client.query("SELECT * FROM tasks_view WHERE grabbed_by = $1 AND task_status = " + TaskStatus.FOREGROUND, [user_id], function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong!");
            });
        } else {
            if (data.rows.length > 0) {
                var task = data.rows[0];
                callback(task);
            } else {
                fail_message(client, event, context, "You have no foreground tasks!");
            }
        }
    });
}

task_status = function(client, event, context, user_id) {
    switch (event.status) {
        case "bg":
            if (event.task_id) {
                get_task_by_id(client, event, context, event.task_id, function(task) {
                    if (task) {
                        if (task.task_status != TaskStatus.FOREGROUND) {
                            fail_message(client, event, context, "This task is not active!");
                        } else if (task.grabbed_by && task.grabbed_by != user_id || task.grabbed_by == null) {
                            fail_message(client, event, context, "This task is not your active task!");
                        } else {
                            suspend_task(client, event, context, user_id, data.Item, function(task) {
                                client.end();
                                context.succeed({state: "ok", task: format_task_item(task)});
                            });
                        }
                    } else {
                        fail_message(client, event, context, "Task is not found!");
                    }
                });
            } else {
                get_foreground_task(client, event, context, user_id, function(task) {
                    suspend_task(client, event, context, user_id, task, function(task) {
                        client.end();
                        context.succeed({state: "ok", task: format_task_item(task)});
                    });
                });
            }
            break;

        case "fg":
            client.query("SELECT * FROM tasks WHERE task_status = $1 AND grabbed_by = $2", [TaskStatus.FOREGROUND, user_id], function(err, data) {
                if (err) {
                    console.log(err);
                    sentry.captureError(err, function(result) {
                        fail_message(client, event, context, "Oops... something went wrong! Try again later");
                    });
                } else {
                    if (data.rows.length > 0) {
                        fail_message(client, event, context, "You already have a foreground task!");
                    } else {
                        if (event.task_id) {
                            client.query("SELECT * FROM tasks WHERE task_id = $1", [event.task_id], function(err, data) {
                                if (err) {
                                    console.log(err);
                                    sentry.captureError(err, function(result) {
                                        fail_message(client, event, context, "Oops.. something went wrong! Try again later");
                                    });
                                } else {
                                    if (data.rows.length > 0) {
                                        if (data.rows[0].task_status != TaskStatus.BACKGROUND) {
                                            fail_message(client, event, context, "This task is not suspended!");
                                        } else if (data.rows[0].grabbed_by && data.rows[0].grabbed_by != user_id || !data.rows[0].grabbed_by) {
                                            fail_message(client, event, context, "This is not your suspended task!");
                                        } else {
                                            activate_task(client, event, context, user_id, data.rows[0], function(task) {
                                                client.end();
                                                context.succeed({state: "ok", task: format_task_item(task)});
                                            });
                                        }
                                    } else {
                                        fail_message(client, event, context, "Task is not found!");
                                    }
                                }
                            });
                        } else {
                            client.query("SELECT * FROM tasks WHERE task_status = $1 AND grabbed_by = $2 ORDER BY suspended_on DESC", [TaskStatus.BACKGROUND, user_id], function (err, data) {
                                if (err) {
                                    console.log(err);
                                    sentry.captureError(err, function(result) {
                                        fail_message(client, event, context, "Oops.. something went wrong! Try again later");
                                    });
                                } else {
                                    if (data.rows.length == 0) {
                                        client.end();
                                        context.succeed({state: "fail", message: "You have no background jobs!"});
                                    } else {
                                        var task = data.rows[0];
                                        activate_task(client, event, context, user_id, task, function(task) {
                                            client.end();
                                            context.succeed({state: "ok", task: format_task_item(task)});
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
            release_task = function(task) {
                var task_id = task.task_id;
                log_event(client, user_id, "release", task.task_id, function() {
                    client.query("UPDATE tasks SET task_status = $1, released_by = $2, released_on = $3, grabbed_by = NULL, grabbed_on = NULL WHERE task_id = $4", [TaskStatus.IN_QUEUE, user_id, (new Date()).getTime(), task_id], function(err, data) {
                        if (err) {
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops, looks like something is wrong... Try again later");
                            });
                        } else {
                            get_task_by_id(client, event, context, task.task_id, function(task) {
                                client.end();
                                context.succeed({state: "ok", task: format_task_item(task)});
                            });
                        }
                    });
                });
            }
            if (event.task_id) {
                client.query("SELECT * FROM tasks WHERE task_id = $1", [event.task_id], function(err, data) {
                    if (err) {
                        console.log(err);
                        sentry.captureError(err, function(result) {
                            fail_message(client, event, context, "Oops.. something went wrong! Try again later");
                        });
                    } else {
                        if (data.rows.length > 0) {
                            if (!(data.rows[0].task_status == TaskStatus.FOREGROUND || data.rows[0].task_status == TaskStatus.BACKGROUND)) {
                                fail_message(client, event, context, "This task cannot be released!");
                            } else if (data.rows[0].grabbed_by && data.rows[0].grabbed_by != user_id || !data.rows[0].grabbed_by) {
                                fail_message(client, event, context, "This task is not yours!");
                            } else {
                                release_task(client, event, task_id);
                            }
                        } else {
                            fail_message(client, event, context, "Task is not found!");
                        }
                    }
                });
            } else {
                get_foreground_task(client, event, context, user_id, function(task) {
                    release_task(task);
                });
            }
            break;

        case "complete-success":
            get_foreground_task(client, event, context, user_id, function(task) {
                log_event(client, user_id, "complete-success", task.task_id, function() {
                    client.query("UPDATE tasks SET task_status = $1, completion_status = $2, completed_by = $3, completed_on = $4, grabbed_by = NULL, grabbed_on = NULL WHERE task_id = $5", [TaskStatus.DONE, CompletionStatus.SUCCESS, user_id, (new Date()).getTime(), task.task_id], function(err, data) {
                        if (err) {
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops, looks like something is wrong... Try again later");
                            });
                        } else {
                            get_task_by_id(client, event, context, task.task_id, function(task) {
                                client.end();
                                context.succeed({state: "ok", task: format_task_item(task)});
                            });
                        }
                    });
                });
            });
            break;

        case "complete-failure":
            get_foreground_task(client, event, context, user_id, function(task) {
                log_event(client, user_id, "complete-failure", task.task_id, function() {
                    client.query("UPDATE tasks SET task_status = $1, completion_status = $2, completed_by = $3, completed_on = $4, grabbed_by = NULL, grabbed_on = NULL WHERE task_id = $5", [TaskStatus.DONE, CompletionStatus.FAILURE, user_id, (new Date()).getTime(), task.task_id], function(err, data) {
                        if (err) {
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops, looks like something is wrong... Try again later");
                            });
                        } else {
                            get_task_by_id(client, event, context, task.task_id, function(task) {
                                client.end();
                                context.succeed({state: "ok", task: format_task_item(task)});
                            });
                        }
                    });
                });
            });
            break;
    }
}

get_available_task = function(client, event, context, tag, n, callback) {
    var query = "SELECT t.* FROM tasks_view t LEFT JOIN tasks dt ON (dt.task_id = t.depends_on) WHERE t.task_status = " + TaskStatus.IN_QUEUE + " AND (dt.task_status IS NULL OR dt.task_status = " + TaskStatus.DONE + ")";
    var parameters = [];
    if (tag != undefined && tag != null && tag != "") {
        query += " AND t.tags @> ARRAY[$1]";
        parameters.push(tag);
    }
    query += " ORDER BY t.priority DESC, t.created_on ASC LIMIT " + n;
    client.query(query, parameters, function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong!");
            });
        } else {
            if (data.rows.length > 0) {
                callback(data.rows);
            } else {
                client.end();
                context.succeed({text: "There seems to be no tasks to work on at this time! Try again later"});
            }
        }
    });
}

list_available = function(client, event, context) {
    var n = 1;
    if (event.n != undefined && event.n != "") {
        var n_tmp = parseInt(event.n);
        if (!isNaN(n_tmp)) {
            n = n_tmp;
        }
    }
    get_available_task(client, event, context, event.tag, n, function(tasks) {
        var result = [];
        for (var task_index = 0; task_index < tasks.length; task_index++) {
            result.push(format_task_item(tasks[task_index]));
        }

        client.end();
        context.succeed(result);
    });
}

grab = function(client, event, context, user_id) {
    grab_task = function(task) {
        client.query("SELECT * FROM tasks WHERE grabbed_by = $1 AND task_status = $2", [user_id, TaskStatus.FOREGROUND], function (err, data) {
            grab_task = function(user_id, task, suspended_task) {
                log_event(client, user_id, event.action, task.task_id, function() {
                    client.query("UPDATE tasks SET task_status = $1, grabbed_on = $2, grabbed_by = $3 WHERE task_id = $4 AND task_status = $5", [TaskStatus.FOREGROUND, (new Date()).getTime(), user_id, task.task_id, TaskStatus.IN_QUEUE], function(err, data) {
                        if (err) {
                            console.log("Failed to grab task");
                            console.log(params);
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops... something went wrong! Please try again later");
                            });
                        } else {
                            if (data.rowCount > 0) {
                                get_task_by_id(client, event, context, task.task_id, function(task) {
                                    var result = {
                                        task: format_task_item(task)
                                    };
                                    if (suspended_task != undefined) {
                                        result["suspended"] = suspended_task.TaskId.N;
                                    }
                                    client.end();
                                    context.succeed(result);
                                });
                            } else {
                                grab(client, event, context, user_id);
                            }
                        }
                    });
                });
            }

            if (data.rows.length > 0) {
                suspend_task(client, event, context, user_id, data.rows[0], function(suspended_task) {
                    grab_task(user_id, data.rows[0], suspended_task);
                });
            } else {
                grab_task(user_id, task);
            }
        });
    }

    if (event.task_id != undefined && event.task_id != "") {
        client.query("SELECT * FROM tasks WHERE task_id = $1", [event.task_id], function(err, data) {
            if (err) {
                console.log("Failed to get task");
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong! Try again later");
                });
            } else {
                if (data.rows.length > 0) {
                    var task = data.rows[0];
                    if (task.task_status != TaskStatus.IN_QUEUE.toString()) {
                        fail_message(client, event, context, "Task is not in queue!");
                    } else {
                        grab_task(task);
                    }
                } else {
                    fail_message(client, event, context, "Task is not found!");
                }
            }
        });
    } else {
        get_available_task(client, event, context, event.tag, 1, function(tasks) {
            grab_task(tasks[0]);
        });
    }
}

who = function(client, event, context) {
    client.query("SELECT * FROM users WHERE user_status = $1", [UserStatus.IS_LOGGEDIN], function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops, looks like something went wrong... Try again later");
            });
        } else {
            var result = [];
            for (var item_index = 0; item_index < data.rows.length; item_index++) {
                var item = data.rows[item_index];
                result.push(get_user_item(item));
            }

            client.end();
            context.succeed(result);
        }
    });
}

finger = function(client, event, context) {
    if (event.email != undefined && event.email != "") {
        client.query("SELECT * FROM users WHERE user_email = $1", [event.email], function(err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops, looks like something is wrong!");
                });
            } else {
                if (data.rows.length > 0) {
                    client.end();
                    context.succeed(get_user_item(data.rows[0]));
                } else {
                    fail_message(client, event, context, "User is not found!");
                }
            }
        });
    } else {
        fail_message(client, event, context, "Please provide an email");
    }
}

users = function(client, event, context) {
    client.query("SELECT * FROM users", [], function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops, looks like something went wrong... Try again later");
            });
        } else {
            var result = [];
            for (var item_index = 0; item_index < data.rows.length; item_index++) {
                var item = data.rows[item_index];
                result.push(get_user_item(item));
            }

            client.end();
            context.succeed(result);
        }
    });
}

purge = function(client, event, context, user_id, user_data) {
    if (event.task_id != undefined && event.task_id != "") {
        if (user_data.super_user) {
            client.query("DELETE FROM events WHERE event_task_id = $1", [event.task_id], function(err, data) {
                client.query("DELETE FROM tasks WHERE task_id = $1", [event.task_id], function(err, data) {
                    if (err) {
                        console.log(err);
                        sentry.captureError(err, function(result) {
                            fail_message(client, event, context, "Oops... something went wrong! Please try again later");
                        });
                    } else {
                        client.end();
                        context.succeed();
                    }
                });
            });
        } else {
            fail_message(client, event, context, "You do not have the privileges to do this!");
        }
    } else {
        fail_message(client, event, context, "You must specify a task ID!");
    }
}

task_delete = function(client, event, context, user_id) {
    if (event.task_id != undefined && event.task_id != "") {
        client.query("SELECT * FROM tasks WHERE task_id = $1", [event.task_id], function (err, data) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Cannot find specified task!");
                });
            } else {
                var task = data.rows[0];
                if (task) {
                    if (task.task_status != TaskStatus.IN_QUEUE) {
                        fail_message(client, event, context, "Task is already being worked on!");
                    } else {
                        log_event(client, user_id, event.action, task.task_id, function() {
                            if (event.tag) {
                                client.query("UPDATE tasks SET task_status = $1, tags = array_append(tags, $2) WHERE task_id = $3", [TaskStatus.DELETED, event.tag, task.task_id], function(err, data) {
                                    if (err) {
                                        console.log(err);
                                        fail_message(client, event, context, "Failed to delete task, please try again later");
                                    } else {
                                        client.end();
                                        context.succeed(true);
                                    }
                                });
                            } else {
                                client.query("UPDATE tasks SET task_status = $1 WHERE task_id = $2", [TaskStatus.DELETED, task.task_id], function(err, data) {
                                    if (err) {
                                        console.log(err);
                                        fail_message(client, event, context, "Failed to delete task, please try again later");
                                    } else {
                                        client.end();
                                        context.succeed(true);
                                    }
                                });
                            }
                        });
                    }
                } else {
                    fail_message(client, event, context, "Cannot find specified task!");
                }
            }
        });
    } else {
        fail_message(client, event, context, "Please provide a task ID");
    }
}

login = function(client, event, context) {
    if (event.email != undefined && event.email != "") {
        client.query("SELECT * FROM users WHERE user_email = $1", [event.email], function(err, result) {
            if (err) {
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Failed to login");
                });
            } else {
                var data = result.rows[0];
                if (data) {
                    var user_id = data.user_id;
                    if (data.super_user && data.effective_user_id) {
                        user_id = data.effective_user_id;
                    }
                    log_event(client, user_id, "login", null, function() {
                        client.query("UPDATE users SET user_status = $1, last_logged_in = $2 WHERE user_id = $3", [UserStatus.IS_LOGGEDIN, (new Date()).getTime(), user_id], function(err, result) {
                            if (err) {
                                console.log(err);
                                fail_message(client, event, context, "Failed to login, please try again later");
                            } else {
                                client.end();
                                context.succeed(true);
                            }
                        });
                    });
                } else {
                    client.query("INSERT INTO users (user_email) VALUES ($1)", [event.email], function(err, result) {
                        if (err) {
                            console.log("Failed to register user on get_user_id " + event.email);
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops... something went wrong!");
                            });
                        } else {
                            login(client, event, context);
                        }
                    });
                }
            }
        });
    } else {
        fail_message(client, event, context, "Please provide an email");
    }
}

logout = function(client, event, context, user_id) {
    client.query("UPDATE users SET user_status = $1 WHERE user_id = $2", [UserStatus.IS_LOGGEDOUT, user_id], function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something failed, please try again later");
            });
        } else {
            log_event(client, user_id, event.action, null, function() {
                client.end();
                context.succeed();
            });
        }
    });
}

sudo = function(client, event, context, user_id, user_data) {
    if (user_data.super_user) {
        log_event(client, user_id, "sudo", null, function() {
            client.query("UPDATE users as u1 SET effective_user_id = u2.user_id FROM users u2 WHERE u1.user_id = $1 AND u2.user_email = $2", [user_id, event.target_user], function(err, data) {
                if (err) {
                    console.log(err);
                    sentry.captureError(err, function(result) {
                        fail_message(client, event, context, "Oops, something went wrong!");
                    });
                } else {
                    client.end();
                    context.succeed(true);
                }
            });
        }, event.target_user);
    } else {
        fail_message(client, event, context, "You don't have the permission to do this!");
    }
}

get_user_id = function(client, event, context, callback, requires_start) {
    if (event.current_user) {
        client.query("SELECT * FROM users WHERE user_email = $1", [event.current_user], function(err, result) {
            if (err) {
                console.log("get_user_id failed!");
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                var user = result.rows[0];
                if (user) {
                    var user_id = user.user_id;
                    if (user.effective_user_id) {
                        user_id = user.effective_user_id;
                    }
                    callback(user_id, user);
                } else {
                    client.query("INSERT INTO users (user_email) VALUES ($1)", [event.current_user], function(err, result) {
                        if (err) {
                            console.log("Failed to register user on get_user_id " + event.current_user);
                            console.log(err);
                            sentry.captureError(err, function(result) {
                                fail_message(client, event, context, "Oops... something went wrong!");
                            });
                        } else {
                            get_user_id(client, event, context, callback, requires_start);
                        }
                    });
                }
            }
        });
    } else {
        callback("");
    }
}

view_type = function(client, event, context) {
    if (event.type_id) {
        client.query("SELECT t.*, uc.user_email as created_user, uo.user_email as owner_user FROM task_types_view t LEFT JOIN users uc ON (uc.user_id = t.type_created_by) LEFT JOIN users uo ON (uo.user_id = t.task_owner) WHERE t.type_id = $1", [event.type_id], function(err, result) {
            if (err) {
                console.log("Failed at view_type");
                console.log(err);
                sentry.captureError(err, function(result) {
                    fail_message(client, event, context, "Oops... something went wrong!");
                });
            } else {
                client.end();
                var type = result.rows[0];
                if (type) {
                    context.succeed({
                        _id: type.type_id,
                        name: type.type_name,
                        _created_on: type.created_on,
                        _created_by: type.created_user,
                        title: type.task_title,
                        title_prefix: type.task_title_prefix,
                        owner: type.owner_user,
                        description: type.task_description,
                        deadline: type.task_deadline,
                        priority: type.priority,
                        estimate: type.estimate,
                        instructions: type.instructions,
                        tags: type.tags,
                        parent: type.parent_type
                    });
                } else {
                    context.succeed("Type not found!");
                }
            }
        });
    } else {
        fail_message(client, event, context, "Please specify a type ID!");
    }
}

list_task_types = function(client, event, context) {
    client.query("SELECT t.*, uc.user_email as created_user, uo.user_email as owner_user FROM task_types_view t LEFT JOIN users uc ON (uc.user_id = t.type_created_by) LEFT JOIN users uo ON (uo.user_id = t.task_owner)", [], function(err, result) {
        if (err) {
            console.log("list_task_types failed!");
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong!");
            });
        } else {
            var results = [];
            for (var row_index = 0; row_index < result.rows.length; row_index++) {
                var type = result.rows[row_index];
                results.push({
                    _id: type.type_id,
                    name: type.type_name,
                    _created_on: type.created_on,
                    _created_by: type.created_user,
                    title: type.task_title,
                    title_prefix: type.task_title_prefix,
                    owner: type.owner_user,
                    description: type.task_description,
                    deadline: type.task_deadline,
                    priority: type.priority,
                    estimate: type.estimate,
                    instructions: type.instructions,
                    tags: type.tags,
                    parent: type.parent_type
                });
            }

            client.end();
            context.succeed(results);
        }
    });
}

list = function(client, event, context) {
    var where_condition = [];
    var query_parameters = [];
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
        query_parameters.push(status);
        where_condition.push("t.task_status = $" + query_parameters.length);
    } else {
        where_condition.push("(t.task_status = " + TaskStatus.IN_QUEUE + ") AND (dt.task_status IS NULL OR dt.task_status = " + TaskStatus.DONE + ")");
    }
    if (event.owner != undefined && event.owner != "") {
        query_parameters.push(event.owner);
        where_condition.push("uo.user_email = $" + query_parameters.length);
    }
    if (event.tag != undefined && event.tag != "") {
        query_parameters.push(event.tag);
        where_condition.push("t.tags @> ARRAY[$" + query_parameters.length + "]");
    }
    var limit = "";
    if (event.n != undefined && event.n != "") {
        var n = parseInt(event.n);
        if (!isNaN(n)) {
            if (n > 0) {
                limit = "LIMIT " + n;
            }
        }
    } else {
        limit = "LIMIT 100";
    }
    var query = "SELECT t.* FROM tasks_view t LEFT JOIN tasks dt ON (dt.task_id = t.depends_on)";
    if (where_condition.length > 0) {
        query += " WHERE (" + where_condition.join(") AND (") + ")";
    }
    query += " ORDER BY t.priority DESC, t.created_on ASC ";
    if (limit) {
        query += " " + limit;
    }
    client.query(query, query_parameters, function(err, data) {
        if (err) {
            console.log(err);
            sentry.captureError(err, function(result) {
                fail_message(client, event, context, "Oops... something went wrong!");
            });
        } else {
            var result = [];
            if (data.rows.length > 0) {
                for (var task_index = 0; task_index < data.rows.length; task_index++) {
                    var task = data.rows[task_index];
                    result.push(format_task_item(task));
                }
            }

            client.end();
            context.succeed(result);
        }
    });
}

handle_nonloggedin = function(client, event, context) {
    switch (event.action) {
        case "login":
            login(client, event, context);
            break;
        case "events":
            events(client, event, context);
            break;
        case "who":
            who(client, event, context);
            break;
        case "users":
            users(client, event, context);
            break;
        case "get":
            get(client, event, context);
            break;
        case "whoami":
            client.end();
            context.succeed({text: "You are currently not logged in!"});
            break;
        case "list":
            list(client, event, context);
            break;
        case "list-task-types":
            list_task_types(client, event, context);
            break;
        case "list-available":
            list_available(client, event, context);
            break;
        case "history":
            history(client, event, context);
            break;
        case "worker-stats":
            worker_stats(client, event, context);
            break;
        case "worker-jobs":
            worker_jobs(client, event, context);
            break;
        case "finger":
            finger(client, event, context);
            break;
        case "view-type":
            view_type(client, event, context);
            break;
        default:
            fail_message(client, event, context, "Action not found, maybe you're not logged in?");
            break;
    }
}

exports.handler = function(event, context) {
    var client = new pg.Client("");
    client.connect(function (err) {
        if (err) {
            console.log("Failed to connect to database");
            console.log(err);
            context.fail("Oops... something went wrong!");
        } else {
            if (event.current_user || event.user_id) {
                get_user_id(client, event, context, function(user_id, user_data) {
                    if (!user_id) {
                        delete event.token;
                        handle_nonloggedin(client, event, context);
                    } else {
                        switch (event.action) {
                            case "add":
                                add(client, event, context, user_id, user_data);
                                break;
                            case "task-status":
                                if (user_data.user_status == UserStatus.IS_LOGGEDIN) {
                                    task_status(client, event, context, user_id);
                                } else {
                                    fail_message(client, event, context, "Please begin your session before doing this.");
                                }
                                break;
                            case "update":
                                update(client, event, context, user_id);
                                break;
                            case "delete":
                                task_delete(client, event, context, user_id);
                                break;
                            case "logout":
                                logout(client, event, context, user_id);
                                break;
                            case "whoami":
                                client.end();
                                context.succeed({text: "You are logged in as " + user_id});
                                break;
                            case "grab":
                                if (user_data.user_status == UserStatus.IS_LOGGEDIN) {
                                    grab(client, event, context, user_id);
                                } else {
                                    fail_message(client, event, context, "Please begin your session before doing this.");
                                }
                                break;
                            case "purge":
                                purge(client, event, context, user_id, user_data);
                                break;
                            case "sudo":
                                sudo(client, event, context, user_id, user_data);
                                break;
                            case "add-type":
                                add_type(client, event, context, user_id, user_data);
                                break;
                            case "update-type":
                                update_type(client, event, context, user_id, user_data);
                                break;
                            default:
                                fail_message(client, event, context, "Action not found, maybe you're trying to login again?");
                                break;
                        }
                    }
                });
            } else {
                handle_nonloggedin(client, event, context);
            }
        }
    });
}


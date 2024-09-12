### Role Transformation

When the role of a node need to be transformed, like a follower want to be a candidate. It put a "transformation event" in the event queue. This event is a special event, after putting, the event queue is locked, and stay locked until the role transformation is done. So in the time between the role transformation decision is made and the transformation is done, the event queue does not receive any event.

### Event queue separation

As the program keep running, the node can go through many rounds of role transformation. Every role has the role has the access to the event queue, so we must make sure that the last round role can not send any event to this round event queue.

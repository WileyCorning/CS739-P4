With regard to the CAP theorem, we will be consistent and partition-tolerant.

Robust to:
- Client crashing (transient)
    - Only begin work once we have sent the problem ID to the client
    - Work should continue in the DS if client goes offline midway
    - Leader should persist solution if it can't be reported immediately

- Leader crashing (transient)
    - Persist progress data
        - this also acts as a reply cache
    

- Worker crashing (transient or permanent)
- Worker slow
- Worker misreporting

## Leader-to-worker protocol

Each worker has to contact the leader when it wants to volunteer.

The leader will later push task blocks to its current workers when needed.
- Before sending an enumeration range, leader must send the specification and problem ID.
- Leader should also be able to cancel a task block if it is no longer needed.

Workers need to communicate results back to the leader.
- Success
- Block completion
- Incremental progress


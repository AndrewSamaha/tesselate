# ADR-0003: Completion and integration

Status: Accepted for M001

A task or run is complete when its configured policy is satisfied, including required independent reviews and human gates, and the completion transition and event have committed. Completion makes no claim that changes are integrated into a repository's primary branch. The final run result lists the reviewed commit IDs and explicit outstanding integration steps. Tesselate does not merge those commits automatically in the MVP. The engineering workflow for this Tesselate repository separately merges accepted feature branches through its review gates.

# Introduction
Domino project is a HBase-Plugin project which aims to provide developers a simpler way to write large scale distributed applications. Those applications mainly include iterative or incremental computations, and due to their large input scale, are very hard to write in ordinary way.

# Model
Domino follows the trigger-based model. The trigger has been widely used in many commercial database systems since 1990s. The core idea is very simple, we setup a a serial of operations (**Action**) based on some predefined events (**Monitor**), when these events happen, and some conditions (**Filter**) are fulfilled, these predefined operations will execute. In research field, we usually name this model the ECA(Event-Condition-Action) model.

Domino gives developers the same model as traditional databases provide except we are running in current popular Cloud environment. This large scale obviously lead to more challenges comparing with traditional occasions, and also ask us to focus on different subjects. For example, in traditional databases, we need to take care of transactions and atomic operations while processing triggers as we need to guarantee the ACID property of databases, however, in current highly distributed environment, ACID is never the necessary component of system softwares.

What we care about while developing Domino is: 
* Is the programming model flexible enough to support different kinds of applications?
* How to make the performance better while providing enough supports for developers.
* How to make programming under such model simpler.

# Architecture
@TBC

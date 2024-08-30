# Smart Ac - Backend Practical Exercise

For a quick context, we have a simple backend service that works with Smart AC air conditioning units installed in
hundreds of thousands of homes around the country. The devices can register themselves and then immediately start
sending data about their environments.

# Tasks for this Exercise

Before beginning, please familiarize yourself with the project and existing code, make sure that you can open the
project and run unit tests, additionally:

- Familiarize yourself [contributor guide](docs/contributor-guide.md) for commits, branch naming, and pull requests.
- Familiarize yourself with requirement for all required tasks.

After reading the requirements and getting familiar with the project, a plan for the day might look like:

* 1 hour - Your first contribution and making a plan
* 2-3 hours - Refactoring to clean but minimalist architecture
* 4-5 hours - Feature development and related tests

⚠️ All tasks and required features are needed for your project to be reviewed, of course, your times may vary so please feel free to ask for extended time if needed, or to reschedule the project for a better time.

## 1. Your First Contribution

The team quickly setup a new project and took care of some of the tasks to get the project moving. They did not quite get it all done and working, so to start please:

- Fix failing tests and make sure all test are running and passing

## 2. Make a Plan

Now that you are familiar with the code, and the tasks and features listed below, please provide a rough plan to the team in Slack about how you are going to proceed for the remaining tasks.

## 3. Refactoring: Layering the Code

We now have a very good understanding of the feature list and that gives us an idea on where this project is heading. The existing code is not layered very well (having all of the code in the controllers) and we would like you to take the lead on improving this before development continues.

The new architecture must be scalable, maintainable and should take advantage of all the performance capabilities of ASP.NET Core. A lot of architectures would work here, but lets find a minimalistic one since this project should be evolvable in the future without the need of a big refactor on the architecture.

- Discuss in Slack any questions that you might have, and share the architecture or changes you are planning to do.
- Implement the architecture and migrate current features to it.
- ⚠️ Time is precious, don't forget there are other tasks and features to be built!

## 4. New Features

With the structure in place, we now want to implement new functionality into our project, the following is a list of those features we want your help on:

### Required Features

- [x]  [BE-DEV-1](docs/smartac-spec.md#be-dev-1) - A device can self-register with the server (_open endpoint, no auth_)
- [x]  [BE-DEV-2](docs/smartac-spec.md#be-dev-2) - A device will continually report its sensor readings to the server (_
  secure endpoint, requires auth_)
- [ ]  [BE-DEV-3](docs/smartac-spec.md#be-dev-3) - Received device data that is out of expected safe ranges should
  produce alerts (_internal logic_)
- [ ]  [BE-DEV-4](docs/smartac-spec.md#be-dev-4) - Device alerts should merge and not duplicate (_internal logic_)
- [ ]  [BE-DEV-5](docs/smartac-spec.md#be-dev-5) - Device alerts may self resolve (_internal logic_)
- [ ]  [BE-DEV-6](docs/smartac-spec.md#be-dev-6) - A device can read its alerts back from the system to display on its own user interface (_
  secure endpoint, requires auth_)

_**note:** a few of the above are already marked as completed and implementations exist in the current code._

_**note:** for in-depth context read the [full project specs](docs/smartac-spec.md)._

# Documentation

Write your own documentation about decisions, assumptions, or any other thing you feel is important here.

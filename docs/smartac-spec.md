# SmartAC Proof of Concept - Specification

## Notes to Candidate:

This is a fictional project for a fictional client, but please treat the project in a realistic fashion including any communication with the team.  

**LICENSE: _All resources included in this project, and communication via Slack are to be treated as confidential and cannot be shared publicly outside of this exercise; either in their original form or as derivations.  You may retain a private copy for your own use only._**

## Situational Summary

One of our clients is considering working with us on a large future project.  They have given us a proof of concept (PoC) to implement that has two goals:

* Show the key concepts of the project to their internal project funding board.
* See how we build code that meets their requirements.

These goals are in _conflict with each other_ as a PoC or prototype typically favors features and showcasing concepts over code quality.  But at TheoremOne, we never sacrifice code quality.  Instead we make clear decisions about scope and simplifying the features to gain the speed of development.  Therefore our product manager has worked with the client to narrow down their scope to the bare minimum, with some optional features that we can build if time allows.  

## Project Summary

Our client is a manufacturer of smart air conditioning devices (SmartAC) that include either cellular modem or WIFI connectivity in order to report back their status at regular intervals to central servers for monitoring.  The broader project will include all the "bells and whistles" of  user and admin portals, facility management, and end-user registration; but we **are not addressing all of this for the PoC**.  The main goals of the PoC are centered around the concepts that:

* A SmartAC device can register itself with the server, unattended by any user, and in a secure fashion
* A SmartAC device can send regular sensor readings to the server in a secure fashion

We have some flexibility in how to implement these features since the devices are not yet created, but we should stay within some constraints that are expected to exist in the device designs:

* Devices can talk **HTTPS** (and HTTP for local testing)
* Devices do not follow redirects (HTTP `301` or `302`)
* Device programming library has a built-in **JSON** serializer/deserializer.
* Devices can verify **JWT tokens**, and any other auth token will be treated as opaque.
* Devices treat numeric values **all** as decimals to two places (i.e. `1.00`, `2.10`, `12.01`).
* Devices know their own serial number burned into ROM and will never forget it.
* Devices know a shared registration secret burned into ROM and will never forget it.
* Devices can record and buffer data for up to about 4MB of data, and will typically send batches of data instead of individual elements of data.
* Devices cannot do much about errors returned from the server, they are Smart devices but dumb in that they have no ability to handle interaction to work around problems.
* Devices can hold other custom data of up to 2MB.  This data may be lost during long power outages (that exceed battery limits) or during firmware updates.  This is where they store things like auth tokens or state about what they have previously sent.

Therefore it is clear we should write a REST API that the device can talk to over HTTPS (or HTTP in development) using data in JSON.  The device memory sizes we can probably safely ignore on the server-side other than ensuring any auth token sent to the device is not excessively large.  Other than that, you can design the format of the JSON objects as needed.

## Features

### Backend Device API (BE-DEV)

The backend device API must allow communication with the devices (register, report status, see alerts) as well as processing of the device data after it is received (validation, alerts).

#### BE-DEV-1

A device can self-register with the server (open endpoint, no auth)

> Using it's own serial-number and a shared secret (known only to the device and to the server) the device can send this information to the server and receive an auth token in response which is to be used in subsequent calls. The device will also report its firmware version during registration.

Notes:  

* The server should have a fixed set of devices known to it for the purposes of testing and this PoC, some 5 to 10 devices with unique secrets each should be added to the database to be used.
* A device might register more than once if it forgets its token, if it receivers a 401 error, or after a firmware update.
* We should store for each registered device:
	* The serial number (alpha numeric 20-32 characters)
	* The date of its first registration (UTC)
	* The date of the most recent registration (UTC)
	* The most recent firmware version, [semantic versioning](https://semver.org/) shows a regex to validate as:
        ```regexp
        ^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$
        ```

#### BE-DEV-2

A device will continually report its sensor readings to the server (secure endpoint, requires auth)

> A registered device will soon after and on a continuing basis report its sensor readings to the server.  This includes the **temperature**, **humidity**, **carbon monoxide**, and its overall **health status value**

Notes:

* Requires device auth 
* A device records a snapshot of all of its sensor readings  at a point in time (typically every minute).
* A device sends these readings in batches with each snapshot having its own original timestamp (UTC).
* #### Range values: 
	* All of the numeric readings follow the rule above of having two decimal places.  The units of measurement are not part of the data and are defined implicitly by the type of reading.  
		* **temperature**  in Celsius (-30.00 to 100.00),
		* **humidity** as percentage (0.00 to 100.00)
		* **carbon monoxide** as parts-per-million _PPM_ (0.00 to 100.00).
* If the numeric values are parseable as numbers, accept values even if outside of the expected ranges.  See BE-DEV-3 for handling of out-of-range values.
* The **health status value** is only one of the following known strings:  `OK`, `needs_filter`,   and `needs_service`
* Any HTTP code that is `2xx` will cause the device to interpret the call as successful and it will erase its buffer.  A return of `401` or `403` will cause it to re-register and resend the data after.  All other return codes will cause a resend of the same data later along with newly acquired data until buffer overflows and data is lost.
* We should store for each snapshot of readings:
	* The serial number of the device making the readings
	* The timestamp (UTC) the device recorded the reading
	* The timestamp (UTC) the server received the reading 
	* The health status value
	* The individual sensor readings (temp, humidity, carbon monoxide)

#### BE-DEV-3 

Received device data that is out of expected safe ranges should produce alerts.

> When data is received from a device, the data should be analyzed and determined if it is "out of range" and if so cause an alert to be created.  

Notes:

* Devices should have control returned to them as quickly as possible when sending sensor readings, so all alert processing should be done async.
* Data that can generated alerts are:
	* OUT OF ACCEPTED RANGE (by sensor): Any values for `temperature`, `carbon monoxide`, or `humidity` that are out of the ranges specified in the [requirements above](#range-values) (data is readable but is out of expected ranges).  
        * Use alert text similar to: _"Sensor xyz reported out of range value"_ (where `xyz` is the name of the sensor)
        * Each sensor is its own alert type and report individual alerts.
	    * A value being out of range below minimum value is same as the value being out of range above maximum value (i.e. reported temperature of -100 or 200 both continue same alert).
		* Range values should be configurable.
    * DANGEROUS CO LEVELS: **Carbon Monoxide** at dangerous levels
        * Set the threshold value to 9.00 PPM and alert on any value higher. 
        * Use alert text similar to: _"CO value has exceeded danger limit"_.  
        * This alert is independent of alerts for out of range values and should report its own alert (i.e. a value of 200.0 is both a dangerous CO level alert and also an out of range value)
		* The threshold should be configurable.
	* POOR HEALTH: **Health status value** that is any other string value other than `OK`.  
        * Use alert text: _"Device is reporting health problem: xyz"_ where `xyz` is the text of the health status enumeration.
* Among other things you might consider important we capture the following data for each alert: 
    * The type of alert (out of range temp, out of range CO, out of range humidity, dangerous CO, poor health)
	* The timestamp (UTC) of when the server created the alert
	* The timestamp (UTC) of the recorded datetime for the sensor data that caused (started) the alert
	* The timestamp (UTC) of the recorded datetime for the sensor data of the most recent reported value within this alert. (This will make more sense after you get familiarized with BE-DEV-4)
	* The alert message (_examples above_)
	* The resolve state of the alert (`new`,  `resolved`)
* An alert provides a connection to the sensor readings that are related to the alert by the serial number and the recorded datetime range of the alert. (This will make more sense after you get familiarized with BE-DEV-4)

#### BE-DEV-4

Device alerts should merge and not duplicate.

> A device that enters an alert state for a given topic, should not duplicate that same alert while the alert is still unresolved.

Notes:

* A device might have ongoing data values that would cause alerts.  For the same alert type, this expands a previously created alert that falls within its timeframe or does create a new alert.
    * It is important to process batch of alerts individually in recorded datetime order to ensure they are merging with the correct alerts.  The same batch might both create and resolve an alert.
    * The resolutions status of `resolved` for an alert followed by a new alert within (a configurable) 15 minutes, merges into previous alert and sets the status back to `new`, while outside that time range creates a new alert instead. This prevents many alerts for boundary values.
* The value causing the alert might differ from previous, but if the specific alert type is the same, these are considered the same alert.  Examples:
	* A device reports dangerous CO levels PPM at 25, then 28, then 30, then 24 - are all considered the same alert of _"CO value has exceeded danger limit"_ and expand the time range of the same alert.
	* A device reports CO PPM at 25, which is out of range.  Then there is a temperature of -500.00 which is out of range.  These are considered two different alerts and do not merge.
    * A device reports temperature 200, then 199, then 180, which causes a temperature out of range alert with merging, causing the time range of the alert to expand for all of these sensor readings.
    * A device reports both a temperature of 200 and a humidity of 500 in the same reading, this creates two distinct alerts.
* Information should be updated to the existing alert to encompass the new sensor values:
	* the timestamp (UTC) of the recorded datetime for the data of the latest reported value within this alert if it is higher than previous value.
    * any other value that makes sense for your implementation of alert tracking.

#### BE-DEV-5

Device alerts may self resolve.

> A device that was previously in an alert status but is no longer in an alert status should resolve its own alert.

Notes:

* A device which has a current alert that was not resolved will mark its own value resolved when it no longer meets the criteria for the alert type that was created. For example:
    * A device reports CO PPM at 25 which creates an alert, then reports a value of 28 which extends the time range of the same current alert, and then later reports CO PPM of 5 which then marks the alert as `resolved`.
    * A device reports a health status of `needs_filter`, creating an alert and then 10 minutes later reports health status of `OK`, resolving the alert.
    * A device in one batch of data reports a humidity reading of 200 and then a reading of 100, this will both create and resolve an alert.

#### BE-DEV-6

A device may read its own alerts.

> A device may read server-side created alerts, so that for the devices that have user interfaces, they can display the information to their users or maintenance crews.

Notes:

* Requires device auth.
* Alerts can be filtered by resolved state: `all`, `new` (default), `resolved`.
* Return alerts in order of newest to oldest.
* Return the data of the alert.
  * If the sensor of the alert has numeric values return the highest and lowest values that have been reported by the readings associated with it.
* Results should be paged (default 50 per page).
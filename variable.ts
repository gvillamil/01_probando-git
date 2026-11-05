Hello Ayano-san / All,
Below are our GIS west and MFT east findings:


Below is the manual only Get BP launch results, which found your 12 source files (with global * wildcard) and  picked-up from your /RECV folder. Only 4 were successful end-to-end and 8 failed to complete with logged error message, which indicated as  “Invalid Data Length” !

Image_1

I updated the partner consumer mailbox setting with RecordFormat=Undefined (as most likely had varying data length records) and then replayed the 8 failed files which then also picked-up and routed downstream completely. 

Image 2

These below 28 were subsequently and mysteriously re-delivered after I had gone offline, not by automation, as the every 30-min batch launch schedule was not yet built nor enabled.  My guess is that GISUAT was refreshed and recycled and selective files had been re-processed momentarily, as it wasn’t an infinite loop and just briefly repetitive.
NOTE: Of the 12 files, only 7 selectively repeated 4 times each !

Imgae 3

Next day, after I noticed above brief test host partial repeat anomaly, I informed my GIS co-worker to also investigate, double-check and make sure wouldn’t re-occur again.  Just 1 file was re-played and issue didn’t repeat and not re-produced again since.

Image 4

I then completed the downstream portion on MFT east, to push the files to their Mexico_GPH new mailbox and channel. Plus, applied their Mexico Authorized User key, for them to perform their own SFTP Get…, which they successfully performed and picked-up the same 41 total files, as well.

Image 5

As Mexico stated, they then processed above files and generated below 40 Acknowledgement returned files, back to MFT east, whom auto-forwarded the same to GIS west.  Once Oracle DB Admin completes GISUAT DB currently happening maintenance, I will provide the remaining evidence that GISUAT also attempted SFTP Put… of the same 40 files, back to GPH test host, which should show in their logs, as well.

An unusual happening that I noticed, is that GIS only sent GPH, 12 ack files back successfully and 28 failed, which map to the error duplicate ones.  My guess is that GPH /SEND folder is not allowed to overwrite duplicate file names; therefore, they failed.  Maybe it’s by their design, to reject them or maybe it’s a missing permission !

Once GISUAT Oracle DB maintenance applied and rebooted, I will gather tomorrow or later today and show that final screenshot, as well !

Image 6

Regards,

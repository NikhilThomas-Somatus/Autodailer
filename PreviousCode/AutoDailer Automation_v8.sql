



ALTER PROCEDURE [dbo].[sp_AutoDialerCampagin] AS

Declare @PatientActivityId int=0,@IC_DateOfLastCall varchar(max) ='',@IC_CallNotes varchar(max) = '' ,@IC_AgentName varchar(max) =''

DROP TABLE ETL_AutoDialer_List_STG

select
*
into ETL_AutoDialer_List_STG
from ETL_AutoDialer_List
where TransferToRc = 0

ALTER TABLE ETL_AutoDialer_List_STG
Add Patientid varchar(500)

UPDATE ETL_AutoDialer_List_STG SET Patientid = b.patientid
from ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.Payerpatientid





TRUNCATE TABLE SRC_AutoLoaderFrontLoader



------- Auto Dialer Code 2

drop table ETL_AutoDialer_TMP;

--5697
BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5697'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN
             

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5697 CMA Schedules - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5697'

COMMIT

--5699 

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5699'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5699 Engagement - Callback - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5699'


COMMIT


--5704

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5704'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5704 Engagement - Not eligible for program - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END




UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5704'

COMMIT

--5734

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5734'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5734 Verbal Consent - CMA + NP Visit Scheduled - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END




UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5734'


COMMIT

--5735

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5735'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5735 Verbal Consent - CMA Not Scheduled - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5735'


COMMIT

--5736

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5736'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5736 Verbal Consent - CMA Scheduled - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5736'

COMMIT

--5723

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5723'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5723 Patient Declined - Existing Similar Service - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 9000,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5723'


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID, 
a.[Patient Activity ID],
1,
9000
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5723'
and b.PatientID is not null  

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5723'


COMMIT

--5724

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5724'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5724 Patient Declined - Family Supports Care - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 9002,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5724'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID, 
a.[Patient Activity ID],
1,
9002
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5724'
and b.PatientID is not null

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5724'

COMMIT

--5728

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5728'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5728 Patient Declined - Too Busy - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 9001,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5728'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID, 
a.[Patient Activity ID],
1,
9001
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5728'
and b.PatientID is not null

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5728'

COMMIT

--5729

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5729'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5729 Patient Declined - Too Healthy - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 9003,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5729'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID, 
a.[Patient Activity ID],
1,
9003
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5729'
and b.PatientID is not null

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5729'

 COMMIT


--5730

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5730'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5730 Patient Declined - Too Many Doctors - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate()
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8998,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5730'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID, 
a.[Patient Activity ID],
1,
8998
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5730'
and b.PatientID is not null

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '2'
and [IC_AgentDisposition] = '5730'

COMMIT

--------------------------------------Auto Dialer Code 5

--5700

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5700'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5700 Engagement - Deceased - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(), ActivityStatusId = 4367
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


/*UPDATE Patient set IsPatientStatus = 1,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5700'*/


/*UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Invalid Contact Info',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5700'*/


/*Insert into SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
Select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5700'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)*/

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5700'


COMMIT


--5701

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5701'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5701 Engagement - Hung Up During Conversation - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid ,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5701'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5701'

COMMIT

--5702

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;



BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5702'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5702 Engagement - Invalid Contact Information - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Invalid Contact Info',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5702'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5702'
group by a.PatientId 


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows =1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID, 
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '5' and [IC_AgentDisposition] = '5702'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5702'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5702'


COMMIT

--5703

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5703'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5703 Engagement - Agent Left Voicemail - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5703'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

select * from lookup
where displayvalue like '%Outreach%'

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5703'


COMMIT

--5705

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5705'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN


             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5705 Engagement - Not Interested, No Reason Given - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 159,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5705'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
b.PatientID,
a.[Patient Activity ID],
1,
159
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5705'
and b.PatientID is not null

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid ,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5705'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5705'


COMMIT

--5706

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;


BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5706'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5706 Engagement - Good Number Wrong Person - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Invalid Contact Info',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5706'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5706'
group by a.PatientId 


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
left join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '5' and [IC_AgentDisposition] = '5706'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5706'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)



UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5706'

COMMIT


--5707

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5707'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN



             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set 
            --Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			--' - 5707 Engagement - Too Busy to Talk - RING AUTODAILER - '+
			--case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5707'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5707'


COMMIT


--5708

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5708'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5708 Engagement - Unable to Leave Message - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5708'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5708'

COMMIT

--5731

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5731'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 5731 Patient Declined - Wont Verify HIPAA - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8999,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a
inner join ETL_AutoDialer_List_STG b on a.Payerpatientid = b.[Member ID]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5731'

INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
a.[Patient Activity ID],
1,
8999
From ETL_AutoDialer_List_STG a
inner join patient b on a.[Member ID] = b.PayerPatientId
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5731'
and a.[Patient Activity ID] is not null

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5731'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '5731'

COMMIT

--6096

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6096'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 6096 Agent - Answering Machine - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6096'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6096'

COMMIT

--6097

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6097'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 6097 Agent - No Answer - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6097'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6097'

COMMIT

--6098

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6098'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 6098 Agent - Fax Machine - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Invalid Contact Info',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6098'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6098'
group by a.PatientId 


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID, 
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '5' and [IC_AgentDisposition] = '6098'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6098'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6098'


COMMIT

--6460

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6460'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 6460 Agent - System Error - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6460'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6460'

COMMIT

--6461

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6461'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 6461 Agent - Disconnected Number - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END

UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Invalid Contact Info',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6461'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6461'
group by a.PatientId 


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
left join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID, 
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '5' and [IC_AgentDisposition] = '6461'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6461'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '5'
and [IC_AgentDisposition] = '6461'

COMMIT

------- Auto Dialer Code 13

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '13'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 13 The agent labeled this number as a fax machine - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The agent labeled this number as a fax machine.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '13'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '13'
group by a.PatientId 


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
right join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID, 
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '13'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '13'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '13'


COMMIT

------- Auto Dialer Code 14

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '14'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 14 The dialer reached an answering machine and played a pre-recorded message. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '14'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '14'

Commit

------- Auto Dialer Code 15

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '15'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 15 The dialer reached a busy signal. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '15'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '15'

COMMIT

--------- Auto Dialer Code 16

--DROP TABLE ETL_AutoDialer_TMP;

--BEGIN TRANSACTION

--Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '16'

--WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
--BEGIN

--             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
--			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
--			 from ETL_AutoDialer_TMP
                       

--             Update Task 
--			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
--			' - 16 The phone network was unable to place the call. - RING AUTODAILER - '+
--			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
--			modifiedon = getutcdate(),
--			modifiedbyuserid = 55
--             where PatientActivityId = @PatientActivityId

--             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
--             where PatientActivityID = @PatientActivityId

--             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
--END


--INSERT INTO SRC_AutoLoaderFrontLoader(
--patientid,
--patientactivityid
--)
--select 
--a.patientid,
--a.[Patient Activity ID]
--from ETL_AutoDialer_List_STG a
--inner join patient b on a.patientid = b.patientid
--inner join patientactivity c on a.[patient activity id] = c.patientactivityid
--where [IC_SystemClassification] = '16'
--and b.OnHold = 0
--and c.activityid = 2296

--UPDATE ETL_AutoDialer_List_STG SET 
--TransferToRc = 1,
--ModifiedOn = getdate()
--where [IC_SystemClassification] = '16'

--COMMIT

------- Auto Dialer Code 17

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '17'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 17 The dialer detected a fax machine. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END

UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The dialer detected a fax machine.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '17'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '17' 
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
right join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '17'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '17'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '17'

COMMIT

------- Auto Dialer Code 18

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '18'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 18 The number the dialer called was invalid. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The number the dialer called was invalid.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '18'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '18'
group by a.PatientId



UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '18'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '18'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '18'


COMMIT

------- Auto Dialer Code 19

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '19'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 19 The dialer reached an answering machine and may have played a Healthplan approved pre-recorded message. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '19'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '19'

COMMIT

------- Auto Dialer Code 20

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '20'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 20 The contact did not answer before the configured timeout. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select 
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '20'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '20'

COMMIT

------- Auto Dialer Code 21

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '21'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 21 The Called Party Number is no longer assigned. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The Called Party Number is no longer assigned.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '21'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '21'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '21'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select
a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '21'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '21'

COMMIT

------- Auto Dialer Code 22

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '22'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 22 The dialer reached a disconnected number. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END

UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The dialer reached a disconnected number.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '22'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '22'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '22'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '22'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '22'


COMMIT

------- Auto Dialer Code 23

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '23'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 23 The dialer did not connect because no circuit or channel was available. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '23'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '23'

COMMIT

------- Auto Dialer Code 26

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '26'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 26 The system suppressed the call because of an entry in the call suppression list that someone entered manually. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '26'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '26'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

COMMIT

------- Auto Dialer Code 27

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '27'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 27 The system suppressed the call through a web service call. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '27'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '27'

COMMIT

------- Auto Dialer Code 28

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '28'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 28 The system suppressed the call through an IVR response. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '28'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '28'

COMMIT

------- Auto Dialer Code 29

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '29'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 29 The system suppressed the call through a script action. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '29'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '29'

COMMIT

------- Auto Dialer Code 30

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '30'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 30 The system suppressed the call because of an entry in the call suppression list that someone added with bulk upload. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid ,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '30'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '30'


COMMIT

------- Auto Dialer Code 31

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '31'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 31 There was a system error and no disposition or classification was available - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid ,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '31'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '31'


COMMIT

------- Auto Dialer Code 32

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '32'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 32 The contact hung up before being connected to an agent. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid ,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '32'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '32'

COMMIT

------- Auto Dialer Code 40

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '40'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 40 The telephone number does not match existing dialing patterns. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The telephone number does not match existing dialing patterns.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '40'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '40'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '40'



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '40'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '40'

COMMIT

------- Auto Dialer Code 55

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '55'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 55 The phone network was unable to place the call (not final). - RING AUTODAILER- '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The phone network was unable to place the call (not final).',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '55'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '55'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '55'



INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '55'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '55'

COMMIT

------- Auto Dialer Code 56

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '56'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 56 The phone network was unable to place the call (non final). - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The phone network was unable to place the call (non final).',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '56'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '56'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '56'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '56'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '56'

COMMIT

------- Auto Dialer Code 57

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '57'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 57 The phone network was unable to place the call (non final). - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The phone network was unable to place the call (non final).',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '57'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '57'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '57'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '57'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '57'

COMMIT

------- Auto Dialer Code 81

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '81'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 81 The agent identified the destination as a fax number by pressing the Fax button in the MAX interface. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The agent identified the destination as a fax number by pressing the Fax button in the MAX interface.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '81'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '81'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '81'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '81'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '81'

COMMIT

------- Auto Dialer Code 82

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '82'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 82 The agent identified the destination as an answering machine by pressing the Answering Machine button in the MAX interface. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '82'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '82'

COMMIT

------- Auto Dialer Code 83

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '83'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 83 The agent identified the destination as a bad number by pressing the Bad Number button in the MAX interface. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'The agent identified the destination as a bad number by pressing the Bad Number button in the MAX interface.',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '83'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '83'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '83'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '83'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)


UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '83'

COMMIT

------- Auto Dialer Code 84

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '84'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 84 The agent identified the destination as an answering machine by pressing the Answering Machine button in the MAX interface. Additionally, the system initialed the default action for Classify with Answering Machine Detection options in the CPA Management page in your Personal Connection ACD skill parameters. - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '84'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '84'

COMMIT

------- Auto Dialer Code 86

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '86'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 86 Agent Override Answering Machine - No Message - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '86'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '86'

COMMIT

------- Auto Dialer Code 87

DROP TABLE ETL_AutoDialer_TMP;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '87'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 87 Agent Override Answering Machine - System Left Message - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '87'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '87'

COMMIT

------- Auto Dialer Code 90

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION


Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '90'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 90 Call Blocked by Carrier Analytics - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Call Blocked by Carrier Analytics',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '90'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '90'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '90'

INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '90'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '90'

COMMIT

------- Auto Dialer Code 91

DROP TABLE ETL_AutoDialer_TMP;
DROP TABLE ETL_AutoDialer_OnHoldPatients;

BEGIN TRANSACTION

Select * into ETL_AutoDialer_TMP from ETL_AutoDialer_List_STG where [IC_SystemClassification] = '91'

WHILE (SELECT COUNT(1) FROM ETL_AutoDialer_TMP) > 0 
BEGIN

             select Top 1 @PatientActivityId=[Patient Activity ID], @IC_DateOfLastCall= cast([IC_DateOfLastCall] as varchar(max)), 
			 @IC_CallNotes=[IC_CallNotes],@IC_AgentName=[IC_AgentName]
			 from ETL_AutoDialer_TMP
                       

             Update Task 
			Set Comments = Comments+ ' '+cast(@IC_DateOfLastCall as varchar(max))+
			' - 91 Call Refused By Customer - RING AUTODAILER - '+
			case when @IC_CallNotes is null then '' else @IC_CallNotes end+' - '+case when @IC_AgentName is null then '' else @IC_AgentName end,
			modifiedon = getutcdate(),
			modifiedbyuserid = 55
             where PatientActivityId = @PatientActivityId

             Update PatientActivity  Set ModifiedByUserId=55, ModifiedOn=getutcdate(),ActivityStatusId = 249
             where PatientActivityID = @PatientActivityId

             delete from ETL_AutoDialer_TMP where [Patient Activity ID]=@PatientActivityId
END


UPDATE patientcontactinformation SET 
ContactInformationStatusID = -112,
Notes = 'Call Refused By Customer',
ModifiedOn = getutcdate(),
ModifiedByUserId =55
From patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid and REPLACE(a.phonenumber,'-','') = b.[Phone Number]
where [IC_SystemClassification] = '91'


select count(1) as rows, a.PatientId 
into ETL_AutoDialer_OnHoldPatients
from patientcontactinformation a
inner join ETL_AutoDialer_List_STG b on a.patientid = b.patientid
where [IC_SystemClassification] = '91'
group by a.PatientId


UPDATE Patient set OnHold = 1,
OnHoldStatusId = 8860,
ModifiedOn = getutcdate(),
ModifiedByUserId =55
from Patient a 
inner join ETL_AutoDialer_OnHoldPatients b on a.patientid = b.patientid
where b.rows = 1


INSERT INTO PatientOnHoldHistory(
PatientID,
PatientProgramID,
OnHold,
OnHoldStatusID
)
SELECT
a.PatientID,
b.PatientActivityID,
1,
8860
From ETL_AutoDialer_OnHoldPatients a
left join PatientActivity  b on a.patientid=b.PatientId 
INNER JOIN ETL_AutoDialer_List_STG c on b.PatientActivityId = c.[Patient Activity ID]
where a.rows =1
and [IC_SystemClassification] = '91'


INSERT INTO SRC_AutoLoaderFrontLoader(
patientid,
patientactivityid
)
select a.patientid,
a.[Patient Activity ID]
from ETL_AutoDialer_List_STG a
inner join patient b on a.patientid = b.patientid
inner join patientactivity c on a.[patient activity id] = c.patientactivityid
where [IC_SystemClassification] = '91'
and (c.activityid = 2296 or c.activityid = 2297 or c.activityid = 2298 or c.activityid = 9755 or c.activityid = 9756)

UPDATE ETL_AutoDialer_List_STG SET 
TransferToRc = 1,
ModifiedOn = getdate()
where [IC_SystemClassification] = '91'

COMMIT

DROP TABLE  SRC_AutoLoaderFrontLoader_Patinet


select distinct patientid into SRC_AutoLoaderFrontLoader_Patinet  from SRC_AutoLoaderFrontLoader


TRUNCATE TABLE SRC_AutoDialer_PatientStatus

while(select count(*) from SRC_AutoLoaderFrontLoader_Patinet) > 0
Begin
	Declare @patient int

	Select top 1 @patient = patientid from SRC_AutoLoaderFrontLoader_Patinet

	INSERT INTO SRC_AutoDialer_PatientStatus(
	Patientid,
	PatientStatus
	)
	Select
	@patient, 
	dbo.getPatientStatusIndicator(@patient) as PatientStatus

	Delete from SRC_AutoLoaderFrontLoader_Patinet
	where patientid = @patient
end


UPDATE SRC_AutoLoaderFrontLoader set patientstatus = b.patientstatus
from SRC_AutoLoaderFrontLoader a
inner join SRC_AutoDialer_PatientStatus b on a.Patientid = b.patientid;



WITH CTE(patientid,PatientActivityid,DUPLICATECount) as (SELECT patientid, PatientActivityid, ROW_NUMBER() OVER (Partition BY patientid, PatientActivityid order by PatientStatus) AS DUPLICATECount
from SRC_AutoLoaderFrontLoader)
DELETE FROM CTE where DUPLICATECount > 1;



--CurrentOutreachAttempt shows the current outreach corresponding to the linked PatientActivityID
INSERT INTO SRC_AutoLoaderFrontLoader_Historical(
Patientid,
PatientActivityID,
PatientStatus,
CurrentOutreachAttempt,
CreatedOn
)
Select 
a.Patientid,
a.PatientActivityID,
PatientStatus,
DisplayValue,
getutcdate() as CreatedOn
from SRC_AutoLoaderFrontLoader a
inner join patientactivity b on a.PatientActivityID = b.PatientActivityID
inner join lookup c on c.lookupid = b.activityid




--exclusion criteria makes sure that for the outreach that needs to be created that its not already open
DELETE a from SRC_AutoLoaderFrontLoader a
inner join patientactivity b on a.patientid = b.patientid
inner join patienteligibility c on a.patientid = c.patientid
inner join patient d on a.Patientid = d.PatientId
where (b.activityid = 2297 and b.activitystatusid = 104)
or (b.activityid = 2298 and b.activitystatusid = 104)
or (b.activityid = 9755 and b.activitystatusid = 104)
or (b.activityid = 9756 and b.activitystatusid = 104)
or (b.activityid = 9757 and b.activitystatusid = 104)
or (c.activepatient = 0)
or (a.patientstatus like 'Written Consent%')
or (a.patientstatus like 'On Hold%' and d.OnHoldStatusId = 8860)
or (a.patientstatus like 'Deceased%')
or (a.patientstatus like 'Verbal Consent%')
or (a.patientstatus like 'Reached%')


-----OutreachAttemptCreated shows which outreach attempt will be created
INSERT INTO SRC_AutoLoaderFrontLoader_AfterDelete_Historical(
Patientid,
PatientActivityID,
PatientStatus,
OutreachAttemptCreated,
CreatedOn
)
Select 
a.Patientid,
a.PatientActivityID,
PatientStatus,
 DisplayValue,
getutcdate() as CreatedOn
from SRC_AutoLoaderFrontLoader a
inner join patientactivity b on a.PatientActivityID = b.PatientActivityID
inner join lookup c on  case when b.activityid = 2296 then 2297
when b.activityid = 2297 then 2298
when b.activityid = 2298 then 9755
when b.activityid = 9755 then 9756
when b.activityid = 9756 then 9757 end  = c.lookupid 


--------------NEW CODE BLOCK---------------------------

--Create Table of HI patients

DROP TABLE SRC_AutoLoaderFrontLoader_HI

select * into SRC_AutoLoaderFrontLoader_HI  from SRC_AutoLoaderFrontLoader
where patientid in(select patientid from PatientAttribute where HighIntensityValue = 1)


--Update HI Task modifiedbyuserid to assigneduserid

DROP TABLE SRC_AutoLoaderFrontLoader_HI_Task

Select * into SRC_AutoLoaderFrontLoader_HI_Task from SRC_AutoLoaderFrontLoader_HI

while(select count(*) from SRC_AutoLoaderFrontLoader_HI_Task) > 0
Begin

select top 1 @PatientActivityId = PatientActivityId from SRC_AutoLoaderFrontLoader_HI_Task

UPDATE TASK SET modifiedbyuserid = assignedtouserid 
where PatientActivityId = @PatientActivityId

Delete from SRC_AutoLoaderFrontLoader_HI_Task
where PatientActivityId = @PatientActivityId

end


--------UPDATE HI patients Patientactivity ModifierByUserID


UPDATE PatientActivity SET a.modifiedbyuserid = c.assignedtouserid 
from PatientActivity a 
inner join SRC_AutoLoaderFrontLoader_HI b on a.patientactivityid = b.patientactivityid
inner join Task c on b.patientactivityid = c.patientactivityid

--Create Non HI patient table

DROP TABLE SRC_AutoLoaderFrontLoader_NON_HI

select * into SRC_AutoLoaderFrontLoader_NON_HI  from SRC_AutoLoaderFrontLoader where patientid not in (select patientid from SRC_AutoLoaderFrontLoader_HI)


--UPDATE NON HI Patients Task Modifiedbyuserid

DROP TABLE SRC_AutoLoaderFrontLoader_Task

select * into SRC_AutoLoaderFrontLoader_Task from SRC_AutoLoaderFrontLoader_NON_HI

while(select count(*) from SRC_AutoLoaderFrontLoader_Task) > 0
Begin


select top 1 @PatientActivityId = PatientActivityId from SRC_AutoLoaderFrontLoader_Task

UPDATE TASK SET modifiedbyuserid = 1574 
where PatientActivityId = @PatientActivityId

Delete from SRC_AutoLoaderFrontLoader_Task
where PatientActivityId = @PatientActivityId

end

--------UPDATE Patientactivity ModifierByUserID

UPDATE PatientActivity SET modifiedbyuserid = 1574 
from PatientActivity a 
inner join SRC_AutoLoaderFrontLoader_NON_HI b on a.patientactivityid = b.patientactivityid

----------END OF NEW CODE BLOCK-------------------


--------------Create Outreach Task #N
BEGIN TRANSACTION 

while(select count(*) from SRC_AutoLoaderFrontLoader) > 0
Begin

declare @taskid int, @patientid int

select top 1 
@patientid =  patientid,
@taskid = b.TaskId
from SRC_AutoLoaderFrontLoader a
inner join task b on a.patientactivityid = b.PatientActivityId

--2nd exclusion criteria updated to ensure no duplicate OR#N are created
IF NOT EXISTS (
SELECT PatientActivityId from PatientActivity where (PatientId=@PatientId and ActivityId=2296 AND ActivityStatusId = 104 AND IsDeleted = 0) 
OR (PatientId=@PatientId and ActivityId=2297 AND ActivityStatusId = 104 AND IsDeleted = 0) 
OR (PatientId=@PatientId and ActivityId=2298 AND ActivityStatusId = 104 AND IsDeleted = 0)
OR (PatientId=@PatientId and ActivityId=9755 AND ActivityStatusId = 104 AND IsDeleted = 0)
OR (PatientId=@PatientId and ActivityId=9756 AND ActivityStatusId = 104 AND IsDeleted = 0)
OR (PatientId=@PatientId and ActivityId=9757 AND ActivityStatusId = 104 AND IsDeleted = 0))
exec sp_RuleEngineOutReachRuleCheck @Taskid = @taskid, @PatientId=@patientid


Delete from SRC_AutoLoaderFrontLoader
where patientid = @patientid
and patientactivityid in (select patientactivityid from task where taskid = @taskid)

end

COMMIT



--Update TransfertoRC flags
UPDATE ETL_AutoDialer_List_STG SET TransferToRC = -1
where TransferToRC = 0

GO



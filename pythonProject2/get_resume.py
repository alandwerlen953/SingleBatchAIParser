from resume_utils import get_resume_by_userid

userid = 1266714
result = get_resume_by_userid(userid)
resume_text = result[1] if isinstance(result, tuple) else result
print(resume_text)

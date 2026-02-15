from resume_utils import get_resume_batch
batch = get_resume_batch(1)
if batch:
    print(f"Found user: {batch[0][0]}")
else:
    print("No users found")

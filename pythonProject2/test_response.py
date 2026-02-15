from resume_utils import get_resume_by_userid, openai, DEFAULT_MODEL, apply_token_truncation
from single_step_processor import create_unified_prompt

userid = 1266714
result = get_resume_by_userid(userid)
resume_text = result[1] if isinstance(result, tuple) else result
print(f'Testing with UserID: {userid}')
print(f'Model: {DEFAULT_MODEL}')

messages = create_unified_prompt(resume_text, userid=userid)
messages = apply_token_truncation(messages)

response = openai.chat.completions.create(
    model=DEFAULT_MODEL,
    messages=messages,
    temperature=0.3
)

print('\n=== FULL LLM RESPONSE ===')
print(response.choices[0].message.content)
print('\n=== TOKEN USAGE ===')
print(f'Input: {response.usage.prompt_tokens}, Output: {response.usage.completion_tokens}')

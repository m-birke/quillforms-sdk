from pydantic import BaseModel


class QuillformsFormResponseMetadata(BaseModel):
    system: str = "quillforms"
    form_id: int
    response_id: int
    submitted_at: str | None
    mail_address: str | None


class QuillformsFormResponsePseudonymized(BaseModel):
    form_id: int
    response_id: int

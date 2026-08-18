// ============================================================
// create-user.dto.ts
//
// WHY IT EXISTS:
//   Data Transfer Object for the POST /users endpoint.
//   class-validator decorators run BEFORE the handler executes,
//   so invalid payloads are rejected with a 400 immediately.
//   This keeps validation logic out of the service layer.
// ============================================================

import { IsEmail, IsNotEmpty, IsString, MinLength } from 'class-validator';

export class CreateUserDto {
  @IsString()
  @IsNotEmpty()
  @MinLength(2)
  name: string;

  @IsEmail()
  @IsNotEmpty()
  email: string;
}

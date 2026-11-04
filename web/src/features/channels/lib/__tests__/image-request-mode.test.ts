/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.
*/
import { describe, expect, test } from 'vitest'

import { IMAGE_REQUEST_MODE } from '../../constants'
import { channelSchema } from '../../types'
import {
  CHANNEL_FORM_DEFAULT_VALUES,
  channelFormSchema,
  transformChannelToFormDefaults,
  transformFormDataToCreatePayload,
  transformFormDataToUpdatePayload,
} from '../channel-form'

describe('image request mode', () => {
  test.each([
    IMAGE_REQUEST_MODE.BOTH,
    IMAGE_REQUEST_MODE.SYNC_ONLY,
    IMAGE_REQUEST_MODE.ASYNC_ONLY,
  ])('round-trips mode %s through channel create and edit', (mode) => {
    const form = channelFormSchema.parse({
      ...CHANNEL_FORM_DEFAULT_VALUES,
      name: 'Image channel',
      key: 'channel-key',
      models: 'image-model',
      image_request_mode: mode,
    })
    const created = transformFormDataToCreatePayload(form).channel
    const updated = transformFormDataToUpdatePayload(form, 7)

    expect(created.image_request_mode).toBe(mode)
    expect(updated.image_request_mode).toBe(mode)

    const channel = channelSchema.parse({
      id: 7,
      name: 'Image channel',
      type: 1,
      key: '',
      status: 1,
      created_time: 1,
      test_time: 0,
      response_time: 0,
      balance_updated_time: 0,
      models: 'image-model',
      group: 'default',
      image_request_mode: mode,
    })
    expect(transformChannelToFormDefaults(channel).image_request_mode).toBe(
      mode
    )
  })

  test('treats a missing legacy channel value as both modes', () => {
    const channel = channelSchema.parse({
      id: 7,
      name: 'Legacy image channel',
      type: 1,
      key: '',
      status: 1,
      created_time: 1,
      test_time: 0,
      response_time: 0,
      balance_updated_time: 0,
      models: 'image-model',
      group: 'default',
    })

    expect(channel.image_request_mode).toBeUndefined()
    expect(transformChannelToFormDefaults(channel).image_request_mode).toBe(
      IMAGE_REQUEST_MODE.BOTH
    )
  })

  test('rejects values outside the supported modes', () => {
    const result = channelFormSchema.safeParse({
      ...CHANNEL_FORM_DEFAULT_VALUES,
      image_request_mode: 3,
    })

    expect(result.success).toBe(false)
  })
})

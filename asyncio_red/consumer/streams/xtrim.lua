-- The goal of this script is to trim messages that have been processed by
-- all extant groups from the a given Redis stream. It returns the number
-- of messages that were deleted from the stream, if any. I make no
-- guarantees about its performance, particularly if the stream is large
-- and not fully processed (so a simple XTRIM isn't possible).

-- First off, bail out early if the stream doesn't exist.
if redis.call("EXISTS", KEYS[1]) == 0 then
  return false
end

-- To figure out what messages are deletable, we fetch the "last-
-- delivered-id" for each consumer group of the stream, and set the lowest
-- one of those ids as our upper bound. Next, we scan the pending lists
-- for each group, because we also don't want to delete any events that
-- are delivered but not acknowledged. The lowest unacknowledged id (if
-- any exists) then becomes our new upper bound.

-- "last-delivered-id" isn't mentioned in the Redis docs, for some reason,
-- but it's in there, as of 5.0.3.

-- In the (common?) case where there are no pending messages, and the
-- lowest last-delivered-id equals the most recent id on the stream, we
-- can just do a simpler (and much more efficient) XTRIM stream MAXLEN 0.
-- If we can't do that, we'll have to pull in all the message ids before
-- the lowest unacknowledged id, and XDEL them all.

-- First, use XINFO GROUPS to get all group names and the most recently
-- distributed ids.
local xinfo_groups = redis.call("XINFO", "GROUPS", KEYS[1])
local last_delivered_ids = {}
local groups = {}

if #xinfo_groups == 0 then
  -- When there's no groups, there's nothing to delete.
  return 0
end

for _, group_info_array in ipairs(xinfo_groups) do
  -- Redis passes us a flattened array of key, value pairs, so before
  -- anything else, convert it into a proper hash-style table so that it's
  -- easier to use.

  local group_info = {}
  for i = 1, #group_info_array, 2 do
    group_info[group_info_array[i]] = group_info_array[i+1]
  end

  table.insert(groups, group_info["name"])
  table.insert(last_delivered_ids, group_info["last-delivered-id"])
end

local lowest_pending_ids = {}

for _, group_name in ipairs(groups) do
  local pending = redis.call("XPENDING", KEYS[1], group_name)

  local lowest_id = pending[2]
  if not lowest_id == false then
    table.insert(lowest_pending_ids, lowest_id)
  end
end

local function string_id_to_table(s)
  local t = {}

  for k, v in string.gmatch(s, "(%d+)-(%d+)") do
    table.insert(t, tonumber(k))
    table.insert(t, tonumber(v))
  end

  return t
end

-- Returns true if a < b, or if a == b (which is important later).
local function compare_ids(a, b)
  local a_t = string_id_to_table(a)
  local b_t = string_id_to_table(b)

  return ((a_t[1] <= b_t[1]) and (a_t[2] <= b_t[2]))
end

table.sort(last_delivered_ids, compare_ids)
table.sort(lowest_pending_ids, compare_ids)

-- Here's our XTRIM optimization.
if #lowest_pending_ids == 0 then
  local stream_info_array = redis.call("XINFO", "STREAM", KEYS[1])

  local stream_info = {}
  for i = 1, #stream_info_array, 2 do
    stream_info[stream_info_array[i]] = stream_info_array[i+1]
  end

  if last_delivered_ids[1] == stream_info["last-generated-id"] then
    -- Yay!
    return redis.call("XTRIM", KEYS[1], "MAXLEN", 0)
  end
end

-- If we've gotten here, looks like we need to do a big XDEL, so find our
-- lower bound.
local lowest_id = last_delivered_ids[1]

-- We can include the lowest delivered id in the deletion, so long as it
-- isn't pending, which we'll check for next.
local protect_lowest_id = false

if #lowest_pending_ids > 0 then
  -- We rely here on compare_ids returning true if the ids are equal.
  if compare_ids(lowest_pending_ids[1], lowest_id) then
    lowest_id = lowest_pending_ids[1]
    protect_lowest_id = true
  end
end

local messages = redis.call("XRANGE", KEYS[1], "-", lowest_id)

if #messages == 0 then
  -- Nothing to delete.
  return 0
end

local delete_command = {"XDEL", KEYS[1]}

for _,t in pairs(messages) do
  local id = t[1]
  if (lowest_id ~= id) or (not protect_lowest_id) then
    table.insert(delete_command, id)
  end
end

-- There's nothing to delete, because no message IDs have
-- been added to the delete command.
if #delete_command == 2 then
  -- Nothing to delete.
  return 0
end

return redis.call(unpack(delete_command))
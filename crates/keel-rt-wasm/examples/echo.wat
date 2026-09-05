;; Runnable component implementing echo.wit; no guest compiler required.
;; The tiny allocator is for this one-call example, not a general guest allocator.
(component
  (core module $m
    (memory (export "memory") 1)
    (func (export "realloc") (param i32 i32 i32 i32) (result i32)
      ;; Reserve bytes 0..8 for the result descriptor; payload starts at 16.
      local.get 3 i32.const 65520 i32.gt_u
      if unreachable end
      i32.const 16)
    (func (export "run") (param $ptr i32) (param $len i32) (result i32)
      i32.const 0 local.get $ptr i32.store
      i32.const 4 local.get $len i32.store
      i32.const 0))
  (core instance $i (instantiate $m))
  (func (export "run") (param "input" (list u8)) (result (list u8))
    (canon lift (core func $i "run")
      (memory $i "memory") (realloc (func $i "realloc")))))

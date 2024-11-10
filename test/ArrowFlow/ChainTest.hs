{-# LANGUAGE OverloadedStrings #-}

module ArrowFlow.ChainTest where

import ArrowFlow.Chain
import Relude
import Test.Tasty.HUnit (Assertion, (@?=))

unit_evalutateSimple :: Assertion
unit_evalutateSimple =
  let f :: Double -> Text = evaluate $ floor <| (singleton (+ (1 :: Integer)) |> show @Text)
   in f 6.4 @?= "7"

evaluate :: Chain (->) a b -> a -> b
evaluate chain x = case viewL chain of
  BaseL f -> f x
  ConsL f chain' -> evaluate chain' (f x)

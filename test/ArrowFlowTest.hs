{-# LANGUAGE Arrows #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module ArrowFlowTest where

import ArrowFlow
import Control.Arrow
import Relude
import Test.Tasty.HUnit
import Text.NonEmpty qualified as NonEmptyText

unit_makeChainSerializableNoError :: Assertion
unit_makeChainSerializableNoError = do
  case makeChainSerializable (toChain exampleFlow) of
    Left errMsg -> assertFailure (toString errMsg)
    Right _ -> pure ()

exampleFlow :: Flow Int Int
exampleFlow = proc x -> do
  y <- arr (+ 1) -< x
  z <-
    if y > 10
      then arrIO (Name $$(NonEmptyText.makeTH "hello")) someIoOperation -< y
      else returnA -< 2
  returnA -< z + 1

someIoOperation :: Int -> IO Int
someIoOperation y = print y >> pure (y * 2)

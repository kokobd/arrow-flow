{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module ArrowFlow.Name (Name (..)) where

import Data.Aeson (FromJSON, ToJSON)
import Relude
import Text.NonEmpty (NonEmptyText)

newtype Name = Name {unName :: NonEmptyText}
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass (FromJSON, ToJSON)

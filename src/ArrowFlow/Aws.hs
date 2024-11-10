{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

module ArrowFlow.Aws
  ( Task (..),
    Parameters,
    fromAmazonkaRequest,
  )
where

import Amazonka qualified
import Amazonka.DynamoDB qualified as DynamoDB
import ArrowFlow.Name (Name (..))
import Data.Aeson qualified as Aeson
import Data.Generics.Labels ()
import GHC.TypeLits (Symbol)
import Relude

newtype Parameters req = Parameters Aeson.Value

fromAmazonkaRequest :: (HasParameters req) => req -> Parameters req
fromAmazonkaRequest = Parameters . toParameters

class (Amazonka.AWSRequest req) => HasParameters req where
  type ResourceArn req :: Symbol
  toParameters :: req -> Aeson.Value

data Task = forall req.
  (HasParameters req) =>
  Task
  { name :: Name,
    parameters :: Parameters req
  }

instance Aeson.ToJSON (Parameters req) where
  toJSON (Parameters v) = v

instance Aeson.FromJSON (Parameters req) where
  parseJSON v = pure $ Parameters v

instance HasParameters DynamoDB.GetItem where
  type ResourceArn DynamoDB.GetItem = "arn:aws:states:::dynamodb:getItem"
  toParameters = Aeson.toJSON

instance HasParameters DynamoDB.PutItem where
  type ResourceArn DynamoDB.PutItem = "arn:aws:states:::dynamodb:putItem"
  toParameters = Aeson.toJSON

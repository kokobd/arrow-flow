{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module ArrowFlow.Chain
  ( Chain,
    singleton,
    pushL,
    (<|),
    pushR,
    (|>),
    connect,
    ViewL (..),
    viewL,
    ViewR (..),
    viewR,
    length,
  )
where

import Data.Sequence qualified as Seq
import Relude hiding (length)
import Unsafe.Coerce (unsafeCoerce)

newtype Chain a b c = Chain (Seq (UnTypedArrow a))

data UnTypedArrow a = forall b c. UnTypedArrow (a b c)

withUntypedArrow :: UnTypedArrow a -> (forall b c. a b c -> r) -> r
withUntypedArrow (UnTypedArrow x) f = f x

singleton :: a b c -> Chain a b c
singleton x = Chain . Seq.singleton $ UnTypedArrow x

data ViewL a b d where
  BaseL :: a b d -> ViewL a b d
  ConsL :: a b c -> Chain a c d -> ViewL a b d

data ViewR a b d where
  BaseR :: a b d -> ViewR a b d
  ConsR :: Chain a b c -> a c d -> ViewR a b d

viewL :: forall a b d. Chain a b d -> ViewL a b d
viewL (Chain (x Seq.:<| xs)) =
  if null xs
    then withUntypedArrow x (\(x' :: a b1 d1) -> BaseL (unsafeCoerce x' :: a b d))
    else withUntypedArrow x (\(x' :: a b1 c1) -> ConsL (unsafeCoerce x' :: a b c) (Chain xs :: Chain a c d))
viewL (Chain Seq.Empty) = error "viewL: empty chain is impossible"

viewR :: forall a b d. Chain a b d -> ViewR a b d
viewR (Chain (xs Seq.:|> x)) =
  if null xs
    then withUntypedArrow x (\(x' :: a b1 d1) -> BaseR (unsafeCoerce x' :: a b d))
    else withUntypedArrow x (\(x' :: a c1 d1) -> ConsR (Chain xs :: Chain a b c) (unsafeCoerce x' :: a c d))
viewR (Chain Seq.Empty) = error "viewR: empty chain is impossible"

pushL :: a b c -> Chain a c d -> Chain a b d
pushL x (Chain xs) = Chain (UnTypedArrow x Seq.<| xs)

infixr 5 <|

(<|) :: a b c -> Chain a c d -> Chain a b d
(<|) = pushL

pushR :: Chain a b c -> a c d -> Chain a b d
pushR (Chain xs) x = Chain (xs Seq.|> UnTypedArrow x)

infixl 5 |>

(|>) :: Chain a b c -> a c d -> Chain a b d
(|>) = pushR

connect :: Chain a b c -> Chain a c d -> Chain a b d
connect (Chain xs) (Chain ys) = Chain (xs Seq.>< ys)

length :: Chain a b c -> Int
length (Chain xs) = Seq.length xs

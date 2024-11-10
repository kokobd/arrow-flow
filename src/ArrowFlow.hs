{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module ArrowFlow
  ( Flow,
    arrIO,
    Name (..),
    PureFunctionException (..),
    evaluateFlowLocally,
    CompiledFlow,
    compileFlow,
    evaluateCompiledFlowLocally,

    -- * Internal
    Extension (..),
    toChain,
    makeChainSerializable,
    showChain,
    Vertex (..),
    ParallelSubChain (..),
  )
where

import ArrowFlow.Aws qualified as Aws
import ArrowFlow.Chain (Chain)
import ArrowFlow.Chain qualified as Chain
import ArrowFlow.Name (Name (..))
import Control.Arrow hiding (first, left, right, second, (&&&))
import Control.Category qualified as C
import Control.Concurrent.Async (concurrently)
import Control.Exception (throwIO)
import Control.Monad.Except (throwError)
import Control.Monad.State.Strict ()
import Data.Aeson (FromJSON, ToJSON)
import Data.Generics.Labels ()
import Relude

data Flow a b where
  Pure :: (a -> Either Text b) -> Flow a b
  Extension :: (FromJSON a, ToJSON a, FromJSON b, ToJSON b) => Extension a b -> Flow a b
  Parallel :: Flow a b -> Flow a b' -> Flow a (b, b')
  Connect :: Flow a b -> Flow b c -> Flow a c
  Choice :: Flow a b -> Flow a' b' -> Flow (Either a a') (Either b b')

data Extension a b where
  ExtInlineIO :: Name -> (a -> IO b) -> Extension a b
  ExtPassThrough :: Extension a a
  ExtAwsTask :: Aws.Task -> Extension a b

instance C.Category Flow where
  id = Pure (pure . C.id)
  (.) :: Flow b c -> Flow a b -> Flow a c
  f . g = Connect g f

instance Arrow Flow where
  arr f = Pure (pure . f)

  (***) :: Flow b c -> Flow b' c' -> Flow (b, b') (c, c')
  f *** g = Parallel (arr fst >>> f) (arr snd >>> g)

  (&&&) :: Flow b c -> Flow b c' -> Flow b (c, c')
  f &&& g = Parallel f g

arrIO ::
  (FromJSON a, ToJSON a, FromJSON b, ToJSON b) =>
  Name ->
  (a -> IO b) ->
  Flow a b
arrIO name f = Extension $ ExtInlineIO name f

instance ArrowChoice Flow where
  (+++) :: Flow b c -> Flow b' c' -> Flow (Either b b') (Either c c')
  f +++ g = Choice f g

-- | Exception that wraps errors returned by pure functions in a flow.
newtype PureFunctionException = PureFunctionException Text
  deriving stock (Typeable, Show, Eq)
  deriving anyclass (Exception)

evaluateFlowLocally :: Flow a b -> a -> IO b
evaluateFlowLocally (Pure f) x = case f x of
  Left errMsg -> throwIO $ PureFunctionException errMsg
  Right y -> pure y
evaluateFlowLocally (Extension (ExtInlineIO _ f)) x = f x
evaluateFlowLocally (Extension ExtPassThrough) x = pure x
evaluateFlowLocally (Extension (ExtAwsTask _)) _ = error "evaluateFlowLocally: ExtAwsTask is not supported"
evaluateFlowLocally (Connect f g) x = do
  x' <- evaluateFlowLocally f x
  evaluateFlowLocally g x'
evaluateFlowLocally (Parallel f g) x = concurrently (evaluateFlowLocally f x) (evaluateFlowLocally g x)
evaluateFlowLocally (Choice f g) x =
  case x of
    Left x' -> Left <$> evaluateFlowLocally f x'
    Right x' -> Right <$> evaluateFlowLocally g x'

data Vertex (p :: Type -> Type -> Type) a b where
  PureVertex :: p a b -> Vertex p a b
  ExtensionVertex :: (FromJSON a, ToJSON a, FromJSON b, ToJSON b) => Extension a b -> Vertex p a b
  ParallelSubChain :: ParallelSubChain p a b c -> Vertex p a (b, c)
  ChoiceSubChain :: ChoiceSubChain p a b c -> Vertex p a (Either b c)

newtype PureVertex a b
  = PureVertex' (a -> Either Text b)

data SerializablePureVertex a b
  = (FromJSON a, ToJSON a, FromJSON b, ToJSON b) =>
    SerializablePureVertex (a -> Either Text b) Int

data ParallelSubChain p a b c where
  ParallelLeftOnly :: Chain (Vertex p) a b -> ParallelSubChain p a b a
  ParallelBothBranches :: Chain (Vertex p) a b -> Chain (Vertex p) a c -> ParallelSubChain p a b c

data ChoiceSubChain p a b c where
  ChoiceLeftOnly :: p a Bool -> Chain (Vertex p) a b -> ChoiceSubChain p a b a
  ChoiceBothBranches :: p a Bool -> Chain (Vertex p) a b -> Chain (Vertex p) a c -> ChoiceSubChain p a b c

showChain :: forall (a :: Type) (b :: Type) (p :: Type -> Type -> Type). Chain (Vertex p) a b -> Text
showChain chain = case Chain.viewR chain of
  Chain.BaseR v -> showVertex v
  Chain.ConsR vs v -> showChain vs <> " >>> " <> showVertex v

showVertex :: forall (a :: Type) (b :: Type) (p :: Type -> Type -> Type). Vertex p a b -> Text
showVertex = \case
  PureVertex _ -> "pure"
  ExtensionVertex _ -> "extension"
  ParallelSubChain _ -> "parallel"
  ChoiceSubChain _ -> "choice"

newtype CompiledFlow a b = CompiledFlow (Chain (Vertex SerializablePureVertex) a b)

compileFlow :: (FromJSON a, ToJSON a, FromJSON b, ToJSON b) => Flow a b -> CompiledFlow a b
compileFlow flow = case makeChainSerializable (toChain flow) of
  Left errMsg ->
    -- This is an impossible code path if there is no bug.
    error $ "Failed to compile flow (detected a bug): " <> show errMsg
  Right x -> CompiledFlow x

evaluateCompiledFlowLocally :: CompiledFlow a b -> a -> IO b
evaluateCompiledFlowLocally (CompiledFlow chain) =
  case Chain.viewL chain of
    Chain.BaseL v -> evaluateVertex v
    Chain.ConsL v vs -> evaluateVertex v >=> evaluateCompiledFlowLocally (CompiledFlow vs)
  where
    evaluateVertex :: Vertex SerializablePureVertex a b -> a -> IO b
    evaluateVertex (PureVertex (SerializablePureVertex f _)) x = case f x of
      Left errMsg -> throwIO $ PureFunctionException errMsg
      Right x' -> pure x'
    evaluateVertex (ExtensionVertex (ExtInlineIO _ f)) x = f x
    evaluateVertex (ExtensionVertex ExtPassThrough) x = pure x
    evaluateVertex (ExtensionVertex (ExtAwsTask _)) _ = error "evaluateVertex: ExtAwsTask is not supported"
    evaluateVertex (ParallelSubChain (ParallelLeftOnly subChain)) x =
      (,x) <$> evaluateCompiledFlowLocally (CompiledFlow subChain) x
    evaluateVertex (ParallelSubChain (ParallelBothBranches left right)) x =
      concurrently
        (evaluateCompiledFlowLocally (CompiledFlow left) x)
        (evaluateCompiledFlowLocally (CompiledFlow right) x)
    evaluateVertex (ChoiceSubChain (ChoiceLeftOnly p subChain)) x = do
      ifM
        (evaluateVertex (PureVertex p) x)
        (Left <$> evaluateCompiledFlowLocally (CompiledFlow subChain) x)
        (pure $ Right x)
    evaluateVertex (ChoiceSubChain (ChoiceBothBranches p left right)) x = do
      ifM
        (evaluateVertex (PureVertex p) x)
        (Left <$> evaluateCompiledFlowLocally (CompiledFlow left) x)
        (Right <$> evaluateCompiledFlowLocally (CompiledFlow right) x)

toChain :: Flow a b -> Chain (Vertex PureVertex) a b
toChain flow =
  case flow of
    Pure f -> Chain.singleton . PureVertex . PureVertex' $ f
    Extension v -> Chain.singleton $ ExtensionVertex v
    Connect f g ->
      let (fChain, gChain) = (toChain f, toChain g)
       in case Chain.viewR fChain of
            Chain.BaseR v -> prependVertex v gChain
            Chain.ConsR vs v -> vs `Chain.connect` prependVertex v gChain
    Parallel left right ->
      initializeParallelSubChain
        (toChain left)
        (toChain right)
        (Chain.singleton . ParallelSubChain)
        (Chain.singleton . PureVertex)
        (\x y -> ParallelSubChain x Chain.<| Chain.singleton (PureVertex y))
    Choice left right ->
      initializeChoiceSubChain
        ( PureVertex' $ \case
            Left _ -> pure True
            Right _ -> pure False
        )
        (prependVertex (PureVertex (PureVertex' expectLeft)) $ toChain left)
        (prependVertex (PureVertex (PureVertex' expectRight)) $ toChain right)
        (Chain.singleton . ChoiceSubChain)
        (Chain.singleton . PureVertex)
        (\x y -> ChoiceSubChain x Chain.<| Chain.singleton (PureVertex y))

prependVertex ::
  forall (a :: Type) (b :: Type) (c :: Type).
  Vertex PureVertex a b ->
  Chain (Vertex PureVertex) b c ->
  Chain (Vertex PureVertex) a c
prependVertex v1 chain =
  case Chain.viewL chain of
    Chain.BaseL v2 -> case mergeVertices v1 v2 of
      MergedOneVertex v -> Chain.singleton v
      MergedTwoVertices v1' v2' _ -> v1' Chain.<| Chain.singleton v2'
    Chain.ConsL v2 chain' -> case mergeVertices v1 v2 of
      MergedOneVertex v -> v Chain.<| chain'
      MergedTwoVertices v1' v2' recursive ->
        v1'
          Chain.<| ( if recursive
                       then prependVertex v2' chain'
                       else v2' Chain.<| chain'
                   )

mergePureVertices ::
  forall (a :: Type) (b :: Type) (c :: Type).
  PureVertex a b ->
  PureVertex b c ->
  PureVertex a c
mergePureVertices (PureVertex' f) (PureVertex' g) = PureVertex' $ f >=> g

data MergeVerticesResult a c where
  MergedOneVertex :: Vertex PureVertex a c -> MergeVerticesResult a c
  MergedTwoVertices :: Vertex PureVertex a b -> Vertex PureVertex b c -> Bool -> MergeVerticesResult a c

mergeVertices ::
  forall (a :: Type) (b :: Type) (c :: Type).
  Vertex PureVertex a b ->
  Vertex PureVertex b c ->
  MergeVerticesResult a c
mergeVertices (PureVertex (PureVertex' f)) (PureVertex (PureVertex' g)) =
  MergedOneVertex . PureVertex . PureVertex' $ f >=> g
mergeVertices f@(PureVertex (PureVertex' f')) (ParallelSubChain chain) =
  case chain of
    ParallelLeftOnly left ->
      let secondPureVertex = PureVertex' $ \(b1, a) -> (b1,) <$> f' a
       in initializeLeftOnlyParallelSubChain
            (prependVertex f left)
            (\subChain -> MergedTwoVertices (ParallelSubChain subChain) (PureVertex secondPureVertex) True)
            (\pureVertex -> MergedOneVertex . PureVertex $ mergePureVertices pureVertex secondPureVertex)
            ( \subChain pureVertex ->
                MergedTwoVertices
                  (ParallelSubChain subChain)
                  (PureVertex $ mergePureVertices pureVertex secondPureVertex)
                  True
            )
    ParallelBothBranches left right ->
      initializeParallelSubChain
        (prependVertex f left)
        (prependVertex f right)
        (MergedOneVertex . ParallelSubChain)
        (MergedOneVertex . PureVertex)
        ( \subChain pureVertex ->
            MergedTwoVertices
              (ParallelSubChain subChain)
              (PureVertex pureVertex)
              True
        )
mergeVertices (PureVertex f@(PureVertex' f')) (ChoiceSubChain chain) =
  case chain of
    ChoiceLeftOnly p left ->
      let secondPureVertex :: PureVertex (Either a1 a) (Either a1 b)
          secondPureVertex = PureVertex' $ \case
            Left x -> pure . Left $ x
            Right x -> do
              y <- f' x
              pure . Right $ y
       in initializeLeftOnlyChoiceSubChain
            (mergePureVertices f p)
            (prependVertex (PureVertex f) left)
            (\subChain -> MergedTwoVertices (ChoiceSubChain subChain) (PureVertex secondPureVertex) True)
            (\pureVertex -> MergedOneVertex . PureVertex $ mergePureVertices pureVertex secondPureVertex)
            ( \subChain pureVertex ->
                MergedTwoVertices
                  (ChoiceSubChain subChain)
                  (PureVertex $ mergePureVertices pureVertex secondPureVertex)
                  True
            )
    ChoiceBothBranches (PureVertex' p) left right ->
      initializeChoiceSubChain
        (PureVertex' $ f' >=> p)
        (prependVertex (PureVertex f) left)
        (prependVertex (PureVertex f) right)
        (MergedOneVertex . ChoiceSubChain)
        (MergedOneVertex . PureVertex)
        ( \subChain pureVertex ->
            MergedTwoVertices
              (ChoiceSubChain subChain)
              (PureVertex pureVertex)
              True
        )
mergeVertices f g =
  MergedTwoVertices f g False

initializeLeftOnlyParallelSubChain ::
  Chain (Vertex PureVertex) a b ->
  (ParallelSubChain PureVertex a b a -> r) ->
  (PureVertex a (b, a) -> r) ->
  (forall c. ParallelSubChain PureVertex a c a -> PureVertex (c, a) (b, a) -> r) ->
  r
initializeLeftOnlyParallelSubChain chain onlySubChain onlyPureVertex pureVertexAfterSubChain =
  case Chain.viewR chain of
    Chain.BaseR (PureVertex (PureVertex' f)) ->
      onlyPureVertex . PureVertex' $ \a -> (,a) <$> f a
    Chain.ConsR chain' (PureVertex (PureVertex' f)) ->
      pureVertexAfterSubChain
        (ParallelLeftOnly chain')
        (PureVertex' $ \(c, a) -> (,a) <$> f c)
    _ -> onlySubChain $ ParallelLeftOnly chain

initializeParallelSubChain ::
  Chain (Vertex PureVertex) a b1 ->
  Chain (Vertex PureVertex) a b2 ->
  (ParallelSubChain PureVertex a b1 b2 -> r) ->
  (PureVertex a (b1, b2) -> r) ->
  (forall c1 c2. ParallelSubChain PureVertex a c1 c2 -> PureVertex (c1, c2) (b1, b2) -> r) ->
  r
initializeParallelSubChain leftChain rightChain onlySubChain onlyPureVertex pureVertexAfterSubChain =
  case (Chain.viewR leftChain, Chain.viewR rightChain) of
    (Chain.BaseR (PureVertex (PureVertex' f)), Chain.BaseR (PureVertex (PureVertex' g))) ->
      onlyPureVertex . PureVertex' $ \a -> (,) <$> f a <*> g a
    (Chain.BaseR (PureVertex (PureVertex' f)), Chain.ConsR chain' (PureVertex (PureVertex' g))) ->
      pureVertexAfterSubChain
        (ParallelLeftOnly chain')
        (PureVertex' $ \(c1, a) -> (,) <$> f a <*> g c1)
    (Chain.BaseR (PureVertex (PureVertex' f)), _) ->
      pureVertexAfterSubChain
        (ParallelLeftOnly rightChain)
        (PureVertex' $ \(c, a) -> (,c) <$> f a)
    (Chain.ConsR chain' (PureVertex (PureVertex' f)), Chain.BaseR (PureVertex (PureVertex' g))) ->
      pureVertexAfterSubChain
        (ParallelLeftOnly chain')
        (PureVertex' $ \(c1, a) -> (,) <$> f c1 <*> g a)
    (Chain.ConsR chain1 (PureVertex (PureVertex' f)), Chain.ConsR chain2 (PureVertex (PureVertex' g))) ->
      pureVertexAfterSubChain
        (ParallelBothBranches chain1 chain2)
        (PureVertex' $ \(c1, c2) -> (,) <$> f c1 <*> g c2)
    (Chain.ConsR chain1 (PureVertex (PureVertex' f)), _) ->
      pureVertexAfterSubChain
        (ParallelBothBranches chain1 rightChain)
        (PureVertex' $ \(c1, c) -> (,) <$> f c1 <*> pure c)
    (_, Chain.BaseR (PureVertex (PureVertex' g))) ->
      pureVertexAfterSubChain
        (ParallelLeftOnly leftChain)
        (PureVertex' $ \(c, a) -> (c,) <$> g a)
    (_, Chain.ConsR chain2 (PureVertex (PureVertex' g))) ->
      pureVertexAfterSubChain
        (ParallelBothBranches leftChain chain2)
        (PureVertex' $ \(c, c2) -> (c,) <$> g c2)
    (_, _) ->
      onlySubChain $
        ParallelBothBranches leftChain rightChain

initializeLeftOnlyChoiceSubChain ::
  PureVertex a Bool ->
  Chain (Vertex PureVertex) a b ->
  (ChoiceSubChain PureVertex a b a -> r) ->
  (PureVertex a (Either b a) -> r) ->
  (forall c. ChoiceSubChain PureVertex a c a -> PureVertex (Either c a) (Either b a) -> r) ->
  r
initializeLeftOnlyChoiceSubChain condition@(PureVertex' conditionF) subChain onlyChoiceSubChain onlyPureVertex pureVertexAfterSubChain =
  case Chain.viewR subChain of
    (Chain.BaseR (PureVertex (PureVertex' f))) ->
      onlyPureVertex . PureVertex' $ \x -> do
        ok <- conditionF x
        if ok
          then Left <$> f x
          else pure (Right x)
    (Chain.ConsR chain (PureVertex (PureVertex' f))) ->
      pureVertexAfterSubChain
        (ChoiceLeftOnly condition chain)
        ( PureVertex' $ \case
            Left x -> Left <$> f x
            Right x -> pure . Right $ x
        )
    _ -> onlyChoiceSubChain $ ChoiceLeftOnly condition subChain

initializeChoiceSubChain ::
  PureVertex a Bool ->
  Chain (Vertex PureVertex) a b1 ->
  Chain (Vertex PureVertex) a b2 ->
  (ChoiceSubChain PureVertex a b1 b2 -> r) ->
  (PureVertex a (Either b1 b2) -> r) ->
  (forall c1 c2. ChoiceSubChain PureVertex a c1 c2 -> PureVertex (Either c1 c2) (Either b1 b2) -> r) ->
  r
initializeChoiceSubChain condition@(PureVertex' conditionF) left right onlyChoiceSubChain onlyPureVertex pureVertexAfterSubChain =
  let inverseCondition = PureVertex' $ fmap not <$> conditionF
   in case (Chain.viewR left, Chain.viewR right) of
        (Chain.BaseR (PureVertex (PureVertex' f)), Chain.BaseR (PureVertex (PureVertex' g))) ->
          onlyPureVertex . PureVertex' $ \x -> do
            ok <- conditionF x
            if ok
              then Left <$> f x
              else Right <$> g x
        (Chain.BaseR (PureVertex (PureVertex' f)), Chain.ConsR chain2 (PureVertex (PureVertex' g))) ->
          pureVertexAfterSubChain
            (ChoiceLeftOnly inverseCondition chain2)
            ( PureVertex' $ \case
                Left x -> Right <$> g x
                Right x -> Left <$> f x
            )
        (Chain.BaseR (PureVertex (PureVertex' f)), _) ->
          pureVertexAfterSubChain
            (ChoiceLeftOnly inverseCondition right)
            ( PureVertex' $ \case
                Left x -> pure . Right $ x
                Right x -> Left <$> f x
            )
        (Chain.ConsR chain1 (PureVertex (PureVertex' f)), Chain.BaseR (PureVertex (PureVertex' g))) ->
          pureVertexAfterSubChain
            (ChoiceLeftOnly condition chain1)
            ( PureVertex' $ \case
                Left x -> Left <$> f x
                Right x -> Right <$> g x
            )
        (Chain.ConsR chain1 (PureVertex (PureVertex' f)), Chain.ConsR chain2 (PureVertex (PureVertex' g))) ->
          pureVertexAfterSubChain
            (ChoiceBothBranches condition chain1 chain2)
            ( PureVertex' $ \case
                Left x -> Left <$> f x
                Right x -> Right <$> g x
            )
        (Chain.ConsR chain1 (PureVertex (PureVertex' f)), _) ->
          pureVertexAfterSubChain
            (ChoiceBothBranches condition chain1 right)
            ( PureVertex' $ \case
                Left x -> Left <$> f x
                Right x -> pure . Right $ x
            )
        (_, Chain.BaseR (PureVertex (PureVertex' g))) ->
          pureVertexAfterSubChain
            (ChoiceLeftOnly condition left)
            ( PureVertex' $ \case
                Left x -> pure . Left $ x
                Right x -> Right <$> g x
            )
        (_, Chain.ConsR chain2 (PureVertex (PureVertex' g))) ->
          pureVertexAfterSubChain
            (ChoiceBothBranches inverseCondition left chain2)
            ( PureVertex' $ \case
                Left x -> pure . Left $ x
                Right x -> Right <$> g x
            )
        (_, _) ->
          onlyChoiceSubChain $ ChoiceBothBranches condition left right

expectLeft :: Either a b -> Either Text a
expectLeft (Left x) = Right x
expectLeft (Right _) = Left "Expecting Left"

expectRight :: Either a b -> Either Text b
expectRight (Left _) = Left "Expecting Right"
expectRight (Right x) = Right x

makeChainSerializable ::
  forall (a :: Type) (b :: Type).
  (FromJSON a, ToJSON a, FromJSON b, ToJSON b) =>
  Chain (Vertex PureVertex) a b ->
  Either Text (Chain (Vertex SerializablePureVertex) a b)
makeChainSerializable chain =
  flip evalStateT 1 $
    makeChainSerializableRec
      (Chain.singleton (ExtensionVertex ExtPassThrough))
      (chain Chain.|> ExtensionVertex ExtPassThrough)

type CompilerM = StateT Int (Either Text)

allocateSerializedPureVertex ::
  (FromJSON a, ToJSON a, FromJSON b, ToJSON b) =>
  PureVertex a b ->
  CompilerM (SerializablePureVertex a b)
allocateSerializedPureVertex (PureVertex' f) = do
  i <- get
  put $ i + 1
  pure $ SerializablePureVertex f i

makeChainSerializableRec ::
  forall (a :: Type) (b :: Type) (c :: Type).
  (FromJSON a, ToJSON a, FromJSON b, ToJSON b) =>
  Chain (Vertex SerializablePureVertex) a b ->
  Chain (Vertex PureVertex) b c ->
  CompilerM (Chain (Vertex SerializablePureVertex) a c)
makeChainSerializableRec converted unconverted =
  case Chain.viewL unconverted of
    Chain.BaseL v ->
      (converted Chain.|>)
        <$> case v of
          PureVertex _ -> throwError "Pure vertex should not be the last vertex in the chain. This is a bug."
          ExtensionVertex v' -> pure $ ExtensionVertex v'
          ParallelSubChain (ParallelLeftOnly subChain) -> do
            ParallelSubChain . ParallelLeftOnly
              <$> makeChainSerializableRec
                (Chain.singleton (ExtensionVertex ExtPassThrough))
                subChain
          ParallelSubChain (ParallelBothBranches left right) -> do
            left' <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) left
            right' <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) right
            pure . ParallelSubChain $ ParallelBothBranches left' right'
          ChoiceSubChain (ChoiceLeftOnly p subChain) -> do
            p' <- allocateSerializedPureVertex p
            ChoiceSubChain . ChoiceLeftOnly p'
              <$> makeChainSerializableRec
                (Chain.singleton (ExtensionVertex ExtPassThrough))
                subChain
          ChoiceSubChain (ChoiceBothBranches p left right) -> do
            p' <- allocateSerializedPureVertex p
            ChoiceSubChain
              <$> ( ChoiceBothBranches p'
                      <$> makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) left
                      <*> makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) right
                  )
    Chain.ConsL v vs ->
      case v of
        PureVertex p -> do
          case Chain.viewL vs of
            Chain.BaseL (ExtensionVertex _) -> do
              p' <- allocateSerializedPureVertex p
              makeChainSerializableRec (converted Chain.|> PureVertex p') vs
            Chain.ConsL (ExtensionVertex _) _ -> do
              p' <- allocateSerializedPureVertex p
              makeChainSerializableRec (converted Chain.|> PureVertex p') vs
            _ -> throwError "pure vertex should always be followed by an effectful vertex"
        ExtensionVertex v' -> makeChainSerializableRec (converted Chain.|> ExtensionVertex v') vs
        ParallelSubChain (ParallelLeftOnly (subChain :: Chain (Vertex PureVertex) b b1)) -> do
          convertedSubChain <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) subChain
          deduceOutputSerializable convertedSubChain $
            makeChainSerializableRec (converted Chain.|> ParallelSubChain (ParallelLeftOnly convertedSubChain)) vs
        ParallelSubChain (ParallelBothBranches left right) -> do
          convertedLeft <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) left
          convertedRight <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) right
          deduceOutputSerializable convertedLeft $
            deduceOutputSerializable convertedRight $
              makeChainSerializableRec
                ( converted
                    Chain.|> ParallelSubChain (ParallelBothBranches convertedLeft convertedRight)
                )
                vs
        ChoiceSubChain (ChoiceLeftOnly p (subChain :: Chain (Vertex PureVertex) b b1)) -> do
          convertedSubChain <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) subChain
          p' <- allocateSerializedPureVertex p
          deduceOutputSerializable convertedSubChain $
            makeChainSerializableRec
              ( converted
                  Chain.|> ChoiceSubChain (ChoiceLeftOnly p' convertedSubChain)
              )
              vs
        ChoiceSubChain (ChoiceBothBranches p left right) -> do
          convertedLeft <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) left
          convertedRight <- makeChainSerializableRec (Chain.singleton (ExtensionVertex ExtPassThrough)) right
          p' <- allocateSerializedPureVertex p
          deduceOutputSerializable convertedLeft $
            deduceOutputSerializable convertedRight $
              makeChainSerializableRec
                ( converted
                    Chain.|> ChoiceSubChain
                      (ChoiceBothBranches p' convertedLeft convertedRight)
                )
                vs

deduceOutputSerializable ::
  forall (a :: Type) (b :: Type) (r :: Type).
  (FromJSON a, ToJSON a) =>
  Chain (Vertex SerializablePureVertex) a b ->
  ((FromJSON b, ToJSON b) => r) ->
  r
deduceOutputSerializable chain f =
  case Chain.viewR chain of
    Chain.BaseR (PureVertex (SerializablePureVertex _ _)) -> f
    Chain.BaseR (ExtensionVertex _) -> f
    Chain.BaseR (ParallelSubChain (ParallelLeftOnly subChain)) -> deduceOutputSerializable subChain f
    Chain.BaseR (ParallelSubChain (ParallelBothBranches left right)) ->
      deduceOutputSerializable left $
        deduceOutputSerializable right f
    Chain.BaseR (ChoiceSubChain (ChoiceLeftOnly _ subChain)) -> deduceOutputSerializable subChain f
    Chain.BaseR (ChoiceSubChain (ChoiceBothBranches _ left right)) ->
      deduceOutputSerializable left $
        deduceOutputSerializable right f
    Chain.ConsR _ (PureVertex (SerializablePureVertex _ _)) -> f
    Chain.ConsR _ (ExtensionVertex _) -> f
    Chain.ConsR vs (ParallelSubChain (ParallelLeftOnly subChain)) ->
      deduceOutputSerializable vs $
        deduceOutputSerializable subChain f
    Chain.ConsR vs (ParallelSubChain (ParallelBothBranches left right)) ->
      deduceOutputSerializable vs $
        deduceOutputSerializable left $
          deduceOutputSerializable right f
    Chain.ConsR vs (ChoiceSubChain (ChoiceLeftOnly _ subChain)) ->
      deduceOutputSerializable vs $
        deduceOutputSerializable subChain f
    Chain.ConsR vs (ChoiceSubChain (ChoiceBothBranches _ left right)) ->
      deduceOutputSerializable vs $
        deduceOutputSerializable left $
          deduceOutputSerializable right f

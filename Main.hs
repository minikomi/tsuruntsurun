{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Data.Attoparsec.ByteString as A
import Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString.Char8 as ACh

import qualified Data.ByteString as B
import Data.ByteString (ByteString)
import Data.Word()

import Data.List (sortBy)
import Data.Ord (comparing)

import Control.Concurrent.STM (atomically)
import qualified Control.Concurrent.STM.TChan as S
import Control.Concurrent.STM.TChan (TChan)
import Control.Concurrent (forkIO)

type MarketType       = B.ByteString
type IssueCode        = B.ByteString
type SeqNo            = Int
type MarketStatusType = B.ByteString

data Header = Header
              MarketType
              IssueCode
              SeqNo
              MarketStatusType
  deriving Show

type Price    = Int
type Quantity = Int

data Bid = Bid Price Quantity
instance Show Bid where
  show (Bid q p) = show q ++"@"++ show p


data Ask = Ask Price Quantity
instance Show Ask where
  show (Ask q p) = show q ++"@"++ show p

type USec = Int

data Packet = Packet {
                      header        :: Header,
                      bidVolume     :: Int,
                      bestBids      :: [Bid],
                      askVolume     :: Int,
                      bestAsks      :: [Ask],
                      bestBidNo     :: String,
                      bestBidQuotes :: [String],
                      bestAskNo     :: String,
                      bestAskQuotes :: [String],
                      timestamp     :: USec
                      }

instance Show Packet where
  show (Packet (Header _ ic _ _) _ bb _ ba _ _ _ _ t) =
    show t ++ " " ++ show ic ++
    show bb ++
    show ba

parseHeader :: Parser Header
parseHeader = do
  marketType       <- A.take 1
  issueCode        <- A.take 12
  seqNo            <- A.count 3 ACh.digit
  marketStatusType <- A.take 2
  return $ Header marketType issueCode (read seqNo) marketStatusType

bidParser :: Parser Bid
bidParser = do
  price    <- A.count 5 ACh.digit
  quantity <- A.count 7 ACh.digit
  return $ Bid (read price) (read quantity)

askParser :: Parser Ask
askParser = do
  price    <- A.count 5 ACh.digit
  quantity <- A.count 7 ACh.digit
  return $ Ask (read price) (read quantity)

timeconvert :: String -> String -> String -> String -> USec
timeconvert  hh mm ss uu = read hh * (60 * 60 * 100) +
                           read mm * (60 * 100) +
                           read ss * 100 +
                           read uu

parsePacket :: Parser Packet
parsePacket = do
  _    <- A.manyTill ACh.anyChar (ACh.string "B603")
  h    <- parseHeader

  bvol <- A.count 7 ACh.digit
  bids <- A.count 5 bidParser

  avol <- A.count 7 ACh.digit
  asks <- A.count 5 askParser

  bestBid <- A.count 5 ACh.digit
  bb1  <- A.count 4 ACh.digit
  bb2  <- A.count 4 ACh.digit
  bb3  <- A.count 4 ACh.digit
  bb4  <- A.count 4 ACh.digit
  bb5  <- A.count 4 ACh.digit

  bestAsk <- A.count 5 ACh.digit
  ba1  <- A.count 4 ACh.digit
  ba2  <- A.count 4 ACh.digit
  ba3  <- A.count 4 ACh.digit
  ba4  <- A.count 4 ACh.digit
  ba5  <- A.count 4 ACh.digit

  hh   <- A.count 2 ACh.digit
  mm   <- A.count 2 ACh.digit
  ss   <- A.count 2 ACh.digit
  uu   <- A.count 2 ACh.digit
  _    <- A.word8 0xff
  return $ Packet h (read bvol) bids (read avol) asks
                  bestBid [bb1, bb2, bb3, bb4, bb5]
                  bestAsk [ba1, ba2, ba3, ba4, ba5]
                  (timeconvert hh mm ss uu)


inputFile :: FilePath
inputFile = "mdf-kospi200.20110216-0.pcap"

data PacketEvent = NewPacket Packet | Flush

writer :: TChan PacketEvent -> IO ()
writer chan = loop [] (0 :: USec)
  where loop pkts maxtime = do pev <- atomically $ S.readTChan chan
                               case pev of
                                NewPacket pkt -> do
                                    let newtime = timestamp pkt
                                        maxtime' = if newtime >= maxtime then newtime else maxtime
                                        (overtime, pkts') = span (\p -> (maxtime' - timestamp p) >= 300) pkts
                                        sortedpkts' = sortBy (comparing timestamp) (pkt : pkts')
                                    mapM_ print overtime
                                    loop sortedpkts' maxtime'
                                Flush -> do mapM_ print pkts
                                            putStrLn "Done"

parseAll :: TChan PacketEvent -> ByteString -> IO()
parseAll chan = loop
  where loop input = case ACh.parse parsePacket input of
                          ACh.Done i p -> do atomically $ S.writeTChan chan (NewPacket p)
                                             loop i
                          _ -> atomically $ S.writeTChan chan Flush

main :: IO ()
main = do
  ch <- S.newTChanIO
  f <- B.readFile inputFile
  _ <- forkIO $ parseAll ch f
  writer ch

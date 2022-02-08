using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Humanizer.Bytes;

namespace BuildBackup
{
    class Program
    {
        private static readonly Uri baseUrl = new Uri("http://us.patch.battle.net:1119/");

        private static string[] checkPrograms;
        private static string[] backupPrograms;

        private static VersionsFile versions;
        private static CdnsFile cdns;
        private static GameBlobFile productConfig;
        private static BuildConfigFile buildConfig;
        private static BuildConfigFile[] cdnBuildConfigs;
        private static CDNConfigFile cdnConfig;
        private static EncodingFile encoding;
        private static InstallFile install;
        private static DownloadFile download;
        private static RootFile root;
        private static PatchFile patch;

        private static bool fullDownload = true;

        private static bool overrideVersions;
        private static string overrideBuildconfig;
        private static string overrideCDNconfig;

        private static readonly Dictionary<string, IndexEntry> indexDictionary = new Dictionary<string, IndexEntry>();
        private static readonly Dictionary<string, IndexEntry> patchIndexDictionary = new Dictionary<string, IndexEntry>();
        private static Dictionary<string, IndexEntry> fileIndexList = new Dictionary<string, IndexEntry>();
        private static Dictionary<string, IndexEntry> patchFileIndexList = new Dictionary<string, IndexEntry>();
        private static readonly ReaderWriterLockSlim cacheLock = new ReaderWriterLockSlim();

        private static readonly CDN cdn = new CDN();

        private static readonly HashSet<string> finishedEncodings = new HashSet<string>();
        private static readonly HashSet<string> finishedCDNConfigs = new HashSet<string>();
        private static readonly SemaphoreSlim downloadThrottler = new SemaphoreSlim(initialCount: 100);

        static async Task Main(string[] args)
        {
            cdn.cacheDir = SettingsManager.cacheDir;
            cdn.client = new HttpClient
            {
                Timeout = new TimeSpan(0, 5, 0)
            };
            cdn.cdnList = new List<string> {
                "blzddist1-a.akamaihd.net", // Akamai first
                //"level3.blizzard.com",      // Level3
                //"us.cdn.blizzard.com",      // Official US CDN
                //"eu.cdn.blizzard.com",      // Official EU CDN
                //"cdn.blizzard.com",         // Official regionless CDN
                //"client01.pdl.wow.battlenet.com.cn", // China 1
                //"client02.pdl.wow.battlenet.com.cn", // China 2
                //"client03.pdl.wow.battlenet.com.cn", // China 3
                //"client04.pdl.wow.battlenet.com.cn", // China 4
                //"client04.pdl.wow.battlenet.com.cn", // China 5
                //"blizzard.nefficient.co.kr", // Korea 
            };

            // Check if cache/backup directory exists
            if (!Directory.Exists(cdn.cacheDir)) { Directory.CreateDirectory(cdn.cacheDir); }

            // Load programs
            if (checkPrograms == null)
            {
                checkPrograms = SettingsManager.checkProducts;
            }

            backupPrograms = SettingsManager.backupProducts;

            foreach (string program in checkPrograms)
            {
                var archiveSizes = new Dictionary<string, uint>();

                if (File.Exists("archiveSizes.txt"))
                {
                    foreach (var line in File.ReadAllLines("archiveSizes.txt"))
                    {
                        var split = line.Split(' ');
                        if (uint.TryParse(split[1], out uint archiveSize))
                        {
                            archiveSizes.Add(split[0], archiveSize);
                        }
                    }
                }

                Console.WriteLine("Using program " + program);

                VersionsFile versions;
                try
                {
                    versions = await GetVersions(program);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error parsing versions: " + e.Message);
                    continue;
                }

                await DownloadMain(versions, program, archiveSizes);

                overrideVersions = true;
                overrideCDNconfig = versions.entries[0].cdnConfig;
            }

            ulong sizeSum = 0;
            await using var outputFile = new StreamWriter("allUrls.txt");
            foreach (var url in cdn.m_allUrls)
            {
                //if (url.Key.Contains("/patch/")) continue;
                
                sizeSum += url.Value;
                await outputFile.WriteLineAsync($"{url.Key} {ByteSize.FromBytes(url.Value)}");
            }

            Console.Out.WriteLine($"total size: {ByteSize.FromBytes(sizeSum)}");
        }

        private static async Task DownloadMain(VersionsFile versions, string program, Dictionary<string, uint> archiveSizes)
        {
            if (versions.entries == null || versions.entries.Length == 0) { Console.WriteLine("Invalid versions file for " + program + ", skipping!"); return; }
            Console.WriteLine("Loaded " + versions.entries.Length + " versions");

            try
            {
                cdns = await GetCDNs(program);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error parsing CDNs: " + e.Message);
            }

            if (cdns.entries == null || cdns.entries.Length == 0) { Console.WriteLine("Invalid CDNs file for " + program + ", skipping!"); return; }
            Console.WriteLine("Loaded " + cdns.entries.Length + " cdns");

            if (!string.IsNullOrEmpty(versions.entries[0].productConfig))
            {
                productConfig = await GetProductConfig(cdns.entries[0].configPath + "/", versions.entries[0].productConfig);
            }

            var decryptionKeyName = "";

            if (productConfig.decryptionKeyName != null && productConfig.decryptionKeyName != string.Empty)
            {
                decryptionKeyName = productConfig.decryptionKeyName;
            }

            if (overrideVersions && !string.IsNullOrEmpty(overrideBuildconfig))
            {
                buildConfig = await GetBuildConfig(cdns.entries[0].path, overrideBuildconfig);
            }
            else
            {
                buildConfig = await GetBuildConfig(cdns.entries[0].path, versions.entries[0].buildConfig);
            }

            // Retrieve all buildconfigs
            for (var i = 0; i < versions.entries.Length; i++)
            {
                await GetBuildConfig(cdns.entries[0].path, versions.entries[i].buildConfig);
            }

            if (string.IsNullOrWhiteSpace(buildConfig.buildName))
            {
                Console.WriteLine("Missing buildname in buildConfig for " + program + ", setting build name!");
                buildConfig.buildName = "UNKNOWN";
            }

            if (overrideVersions && !string.IsNullOrEmpty(overrideCDNconfig))
            {
                cdnConfig = await GetCDNconfig(cdns.entries[0].path, overrideCDNconfig);
            }
            else
            {
                cdnConfig = await GetCDNconfig(cdns.entries[0].path, versions.entries[0].cdnConfig);
            }

            if (cdnConfig.builds != null)
            {
                cdnBuildConfigs = new BuildConfigFile[cdnConfig.builds.Length];
            }
            else if (cdnConfig.archives != null)
            {
                //Console.WriteLine("CDNConfig loaded, " + cdnConfig.archives.Count() + " archives");
            }
            else
            {
                Console.WriteLine("Invalid cdnConfig for " + program + "!");
                return;
            }

            if (!string.IsNullOrEmpty(versions.entries[0].keyRing))
                await cdn.Get(cdns.entries[0].path + "/config/" + versions.entries[0].keyRing[0] + versions.entries[0].keyRing[1] + "/" + versions.entries[0].keyRing[2] + versions.entries[0].keyRing[3] + "/" + versions.entries[0].keyRing);

            if (!backupPrograms.Contains(program))
            {
                Console.WriteLine("No need to backup, moving on..");
                return;
            }

            if (!string.IsNullOrEmpty(decryptionKeyName) && cdnConfig.archives == null) // Let us ignore this whole encryption thing if archives are set, surely this will never break anything and it'll back it up perfectly fine.
            {
                if (!File.Exists(decryptionKeyName + ".ak"))
                {
                    Console.WriteLine("Decryption key is set and not available on disk, skipping.");
                    return;
                }
            }

            Console.Write("Downloading patch files..");
            if (!string.IsNullOrEmpty(buildConfig.patch))
                patch = await GetPatch(cdns.entries[0].path + "/", buildConfig.patch, true);

            if (!string.IsNullOrEmpty(buildConfig.patchConfig))
                await cdn.Get(cdns.entries[0].path + "/config/" + buildConfig.patchConfig[0] + buildConfig.patchConfig[1] + "/" + buildConfig.patchConfig[2] + buildConfig.patchConfig[3] + "/" + buildConfig.patchConfig);
            Console.Write("..done\n");

            if (!finishedCDNConfigs.Contains(versions.entries[0].cdnConfig))
            {
                Console.WriteLine("CDN config " + versions.entries[0].cdnConfig + " has not been loaded yet, loading..");
                Console.Write("Loading " + cdnConfig.archives.Length + " indexes..");
                GetIndexes(cdns.entries[0].path, cdnConfig.archives);
                Console.Write("..done\n");

                if (fullDownload)
                {
                    await DownloadCdnData(downloadThrottler, archiveSizes);
                }
                else
                {
                    Console.WriteLine("Not a full run, skipping archive downloads..");
                    if (!finishedCDNConfigs.Contains(versions.entries[0].cdnConfig)) { finishedCDNConfigs.Add(versions.entries[0].cdnConfig); }
                }
            }

            if (!finishedEncodings.Add(buildConfig.encoding[1]))
            {
                Console.WriteLine("Encoding file " + buildConfig.encoding[1] + " already loaded, skipping rest of product loading..");
                return;
            }

            Console.Write("Loading encoding..");

            try
            {
                if (buildConfig.encodingSize == null || buildConfig.encodingSize.Length < 2)
                {
                    encoding = await GetEncoding(cdns.entries[0].path, buildConfig.encoding[1], 0);
                }
                else
                {
                    encoding = await GetEncoding(cdns.entries[0].path, buildConfig.encoding[1], int.Parse(buildConfig.encodingSize[1]));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Fatal error occurred during encoding parsing: " + e.Message);
                return;
            }

            Dictionary<string, string> hashes = new Dictionary<string, string>();

            string rootKey = "";
            string downloadKey = "";
            string installKey = "";

            if (buildConfig.install.Length == 2)
            {
                installKey = buildConfig.install[1];
            }

            if (buildConfig.download.Length == 2)
            {
                downloadKey = buildConfig.download[1];
            }

            foreach (var entry in encoding.aEntries)
            {
                if (entry.cKey == buildConfig.root.ToUpper()) { rootKey = entry.eKeys[0].ToLower(); }
                if (downloadKey == "" && entry.cKey == buildConfig.download[0].ToUpper()) { downloadKey = entry.eKeys[0].ToLower(); }
                if (installKey == "" && entry.cKey == buildConfig.install[0].ToUpper()) { installKey = entry.eKeys[0].ToLower(); }

                hashes.TryAdd(entry.eKeys[0], entry.cKey);
            }

            Console.Write("..done\n");

            if (true)
            {
                //Console.Write("Loading root..");
                //if (rootKey == "") { Console.WriteLine("Unable to find root key in encoding!"); } else { root = GetRoot(cdns.entries[0].path + "/", rootKey, false); }
                //Console.Write("..done\n");

                Console.Write("Loading download..");
                if (downloadKey == "") { Console.WriteLine("Unable to find download key in encoding!"); } else { download = await GetDownload(cdns.entries[0].path, downloadKey, false); }
                Console.Write("..done\n");

                Console.Write("Loading install..");
                Console.Write("..done\n");

                try
                {
                    if (installKey == "") { Console.WriteLine("Unable to find install key in encoding!"); } else { install = await GetInstall(cdns.entries[0].path, installKey, false); }

                }
                catch (Exception e)
                {
                    Console.WriteLine("Error loading install: " + e.Message);
                }
            }

            if (!fullDownload)
            {
                Console.WriteLine("Not a full run, skipping rest of download..");
                return;
            }

            foreach (var entry in indexDictionary)
            {
                hashes.Remove(entry.Key.ToUpper());
            }

            if (finishedCDNConfigs.Add(versions.entries[0].cdnConfig))
            {
                await DownloadAllKindsOfFileIndexes(downloadThrottler);
            }

            // Unarchived files -- files in encoding but not in indexes. Can vary per build!
            Console.Write("Downloading " + hashes.Count + " unarchived files..");

            var unarchivedFileTasks = new List<Task>();
            foreach (var entry in hashes)
            {
                await downloadThrottler.WaitAsync();
                unarchivedFileTasks.Add(
                    Task.Run(async () =>
                    {
                        try
                        {
                            await cdn.Get(cdns.entries[0].path + "/data/" + entry.Key[0] + entry.Key[1] + "/" + entry.Key[2] + entry.Key[3] + "/" + entry.Key, false);
                        }
                        finally
                        {
                            downloadThrottler.Release();
                        }
                    }));
            }
            await Task.WhenAll(unarchivedFileTasks);
            Console.Write("..done\n");

            await DownloadUnarchivedPatchStuff(downloadThrottler);

            GC.Collect();
        }

        private static async Task DownloadUnarchivedPatchStuff(SemaphoreSlim downloadThrottler)
        {
            if (cdnConfig.patchArchives == null)
            {
                return;
            }
            if (patch.blocks == null)
            {
                return;
            }
            var unarchivedPatchKeyList = new List<string>();
            foreach (var block in patch.blocks)
            {
                foreach (var fileBlock in block.files)
                {
                    foreach (var patch in fileBlock.patches)
                    {
                        var pKey = BitConverter.ToString(patch.patchEncodingKey).Replace("-", "");
                        if (!patchIndexDictionary.ContainsKey(pKey))
                        {
                            unarchivedPatchKeyList.Add(pKey);
                        }
                    }
                }
            }

            if (unarchivedPatchKeyList.Count <= 0)
            {
                return;
            }

            Console.Write("Downloading " + unarchivedPatchKeyList.Count + " unarchived patch files..");
            var unarchivedPatchFileTasks = new List<Task>();
            foreach (var entry in unarchivedPatchKeyList)
            {
                await downloadThrottler.WaitAsync();
                unarchivedPatchFileTasks.Add(
                    Task.Run(async () =>
                    {
                        try
                        {
                            await cdn.Get(cdns.entries[0].path + "/patch/" + entry[0] + entry[1] + "/" + entry[2] + entry[3] + "/" + entry, false);
                        }
                        finally
                        {
                            downloadThrottler.Release();
                        }
                    }));
            }
            await Task.WhenAll(unarchivedPatchFileTasks);

            Console.Write("..done\n");
        }

        private static async Task DownloadAllKindsOfFileIndexes(SemaphoreSlim downloadThrottler)
        {
            if (!string.IsNullOrEmpty(cdnConfig.fileIndex))
            {
                Console.Write("Parsing file index..");
                fileIndexList = await ParseIndex(cdns.entries[0].path + "/", cdnConfig.fileIndex);
                Console.Write("..done\n");
            }

            if (!string.IsNullOrEmpty(cdnConfig.fileIndex))
            {
                Console.Write("Downloading " + fileIndexList.Count + " unarchived files from file index..");

                var fileIndexTasks = new List<Task>();
                foreach (var entry in fileIndexList.Keys)
                {
                    await downloadThrottler.WaitAsync();
                    fileIndexTasks.Add(
                        Task.Run(async () =>
                        {
                            try
                            {
                                await cdn.Get(
                                    cdns.entries[0].path + "/data/" + entry[0] + entry[1] + "/" + entry[2] +
                                    entry[3] + "/" + entry, false, false, fileIndexList[entry].size, true);
                            }
                            finally
                            {
                                downloadThrottler.Release();
                            }
                        }));
                }

                await Task.WhenAll(fileIndexTasks);

                Console.Write("..done\n");
            }

            if (!string.IsNullOrEmpty(cdnConfig.patchFileIndex))
            {
                Console.Write("Parsing patch file index..");
                patchFileIndexList = await ParseIndex(cdns.entries[0].path + "/", cdnConfig.patchFileIndex, "patch");
                Console.Write("..done\n");
            }

            if (!string.IsNullOrEmpty(cdnConfig.patchFileIndex))
            {
                Console.Write("Downloading " + patchFileIndexList.Count + " unarchived patch files from patch file index..");

                var patchFileTasks = new List<Task>();
                foreach (var entry in patchFileIndexList.Keys)
                {
                    await downloadThrottler.WaitAsync();
                    patchFileTasks.Add(
                        Task.Run(async () =>
                        {
                            try
                            {
                                await cdn.Get(
                                    cdns.entries[0].path + "/patch/" + entry[0] + entry[1] + "/" + entry[2] +
                                    entry[3] + "/" + entry, false);
                            }
                            finally
                            {
                                downloadThrottler.Release();
                            }
                        }));
                }

                await Task.WhenAll(patchFileTasks);

                Console.Write("..done\n");
            }

            if (cdnConfig.patchArchives != null)
            {
                Console.Write("Downloading " + cdnConfig.patchArchives.Length + " patch archives..");

                var patchArchiveTasks = new List<Task>();
                foreach (var archive in cdnConfig.patchArchives)
                {
                    await downloadThrottler.WaitAsync();
                    patchArchiveTasks.Add(
                        Task.Run(async () =>
                        {
                            try
                            {
                                await cdn.Get(
                                    cdns.entries[0].path + "/patch/" + archive[0] + archive[1] + "/" +
                                    archive[2] + archive[3] + "/" + archive, false, false, 0, true);
                            }
                            finally
                            {
                                downloadThrottler.Release();
                            }
                        }));
                }

                await Task.WhenAll(patchArchiveTasks);
                Console.Write("..done\n");

                Console.Write("Downloading " + cdnConfig.patchArchives.Length + " patch archive indexes..");
                GetPatchIndexes(cdns.entries[0].path, cdnConfig.patchArchives);
                Console.Write("..done\n");
            }
        }

        private static async Task DownloadCdnData(SemaphoreSlim downloadThrottler, Dictionary<string, uint> archiveSizes)
        {
            Console.Write("Fetching and saving archive sizes..");

            ulong totalSize = 0;

            for (short i = 0; i < cdnConfig.archives.Length; i++)
            {
                var archive = cdnConfig.archives[i];
                if (!archiveSizes.TryGetValue(archive, out var remoteFileSize))
                {
                    remoteFileSize = await cdn.GetRemoteFileSize(cdns.entries[0].path + "/data/" + archive[0] + archive[1] + "/" + archive[2] + archive[3] + "/" + archive);
                    archiveSizes.Add(archive, remoteFileSize);
                }

                totalSize += remoteFileSize;
            }

            var archiveSizesLines = new List<string>();
            foreach (var archiveSize in archiveSizes)
            {
                archiveSizesLines.Add(archiveSize.Key + " " + archiveSize.Value);
            }

            await File.WriteAllLinesAsync("archiveSizes.txt", archiveSizesLines);

            Console.WriteLine($"..done. total size: {totalSize}");

            Console.Write("Downloading " + cdnConfig.archives.Length + " archives..");

            var archiveTasks = new List<Task>();
            for (short i = 0; i < cdnConfig.archives.Length; i++)
            {
                var archive = cdnConfig.archives[i];
                await downloadThrottler.WaitAsync();
                archiveTasks.Add(
                    Task.Run(async () =>
                    {
                        try
                        {
                            uint archiveSize = 0;
                            if (archiveSizes.ContainsKey(archive))
                            {
                                archiveSize = archiveSizes[archive];
                            }

                            await cdn.Get(cdns.entries[0].path + "/data/" + archive[0] + archive[1] + "/" + archive[2] + archive[3] + "/" + archive, false, false, archiveSize, true);
                        }
                        finally
                        {
                            downloadThrottler.Release();
                        }
                    }));
            }
            await Task.WhenAll(archiveTasks);
            Console.Write("..done\n");
        }

        private static async Task<CDNConfigFile> GetCDNconfig(string url, string hash)
        {
            string content;
            var cdnConfig = new CDNConfigFile();

            try
            {
                content = Encoding.UTF8.GetString(await cdn.Get(url + "/config/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash));
            }
            catch (Exception e)
            {
                Console.WriteLine("Error retrieving CDN config: " + e.Message);
                return cdnConfig;
            }

            var cdnConfigLines = content.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

            for (var i = 0; i < cdnConfigLines.Length; i++)
            {
                if (cdnConfigLines[i].StartsWith("#") || cdnConfigLines[i].Length == 0) { continue; }
                var cols = cdnConfigLines[i].Split(new string[] { " = " }, StringSplitOptions.RemoveEmptyEntries);
                switch (cols[0])
                {
                    case "archives":
                        var archives = cols[1].Split(' ');
                        cdnConfig.archives = archives;
                        break;
                    case "archive-group":
                        cdnConfig.archiveGroup = cols[1];
                        break;
                    case "patch-archives":
                        if (cols.Length > 1)
                        {
                            var patchArchives = cols[1].Split(' ');
                            cdnConfig.patchArchives = patchArchives;
                        }
                        break;
                    case "patch-archive-group":
                        cdnConfig.patchArchiveGroup = cols[1];
                        break;
                    case "builds":
                        var builds = cols[1].Split(' ');
                        cdnConfig.builds = builds;
                        break;
                    case "file-index":
                        cdnConfig.fileIndex = cols[1];
                        break;
                    case "file-index-size":
                        cdnConfig.fileIndexSize = cols[1];
                        break;
                    case "patch-file-index":
                        cdnConfig.patchFileIndex = cols[1];
                        break;
                    case "patch-file-index-size":
                        cdnConfig.patchFileIndexSize = cols[1];
                        break;
                    default:
                        //Console.WriteLine("!!!!!!!! Unknown cdnconfig variable '" + cols[0] + "'");
                        break;
                }
            }

            return cdnConfig;
        }

        private static async Task<VersionsFile> GetVersions(string program)
        {
            string content;
            var versions = new VersionsFile();

            if (!SettingsManager.useRibbit)
            {
                using HttpResponseMessage response = await cdn.client.GetAsync(new Uri(baseUrl + program + "/" + "versions"));
                if (response.IsSuccessStatusCode)
                {
                    using HttpContent res = response.Content;
                    content = await res.ReadAsStringAsync();
                }
                else
                {
                    Console.WriteLine("Error during retrieving HTTP versions: Received bad HTTP code " + response.StatusCode);
                    return versions;
                }
            }
            else
            {
                try
                {
                    var client = new Ribbit.Protocol.Client(Ribbit.Constants.Region.EU);
                    var request = client.Request("v1/products/" + program + "/versions");
                    content = request.ToString();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error during retrieving Ribbit versions: " + e.Message + ", trying HTTP..");
                    try
                    {
                        using HttpResponseMessage response = await cdn.client.GetAsync(new Uri(baseUrl + program + "/" + "versions"));
                        if (response.IsSuccessStatusCode)
                        {
                            using HttpContent res = response.Content;
                            content = await res.ReadAsStringAsync();
                        }
                        else
                        {
                            Console.WriteLine("Error during retrieving HTTP versions: Received bad HTTP code " + response.StatusCode);
                            return versions;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error retrieving versions: " + ex.Message);
                        return versions;
                    }
                    return versions;
                }
            }

            return ParseVersions(content);
        }

        private static VersionsFile ParseVersions(string content)
        {
            content = content.Replace("\0", "");
            var lines = content.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

            var lineList = new List<string>();

            for (var i = 0; i < lines.Count(); i++)
            {
                if (lines[i][0] != '#')
                {
                    lineList.Add(lines[i]);
                }
            }

            lines = lineList.ToArray();

            if (lines.Length == 0)
            {
                return versions;
            }
            versions.entries = new VersionsEntry[lines.Length - 1];

            var cols = lines[0].Split('|');

            for (var c = 0; c < cols.Length; c++)
            {
                var friendlyName = cols[c].Split('!').ElementAt(0);

                for (var i = 1; i < lines.Length; i++)
                {
                    var row = lines[i].Split('|');

                    switch (friendlyName)
                    {
                        case "Region":
                            versions.entries[i - 1].region = row[c];
                            break;
                        case "BuildConfig":
                            versions.entries[i - 1].buildConfig = row[c];
                            break;
                        case "CDNConfig":
                            versions.entries[i - 1].cdnConfig = row[c];
                            break;
                        case "Keyring":
                        case "KeyRing":
                            versions.entries[i - 1].keyRing = row[c];
                            break;
                        case "BuildId":
                            versions.entries[i - 1].buildId = row[c];
                            break;
                        case "VersionName":
                        case "VersionsName":
                            versions.entries[i - 1].versionsName = row[c].Trim('\r');
                            break;
                        case "ProductConfig":
                            versions.entries[i - 1].productConfig = row[c];
                            break;
                        default:
                            Console.WriteLine("!!!!!!!! Unknown versions variable '" + friendlyName + "'");
                            break;
                    }
                }
            }

            return versions;
        }

        private static async Task<CdnsFile> GetCDNs(string program)
        {
            string content;

            var cdns = new CdnsFile();

            if (!SettingsManager.useRibbit)
            {
                using HttpResponseMessage response = await cdn.client.GetAsync(new Uri(baseUrl + program + "/" + "cdns"));
                if (response.IsSuccessStatusCode)
                {
                    using HttpContent res = response.Content;
                    content = await res.ReadAsStringAsync();
                }
                else
                {
                    Console.WriteLine("Error during retrieving HTTP cdns: Received bad HTTP code " + response.StatusCode);
                    return cdns;
                }
            }
            else
            {

                try
                {
                    var client = new Ribbit.Protocol.Client(Ribbit.Constants.Region.US);
                    var request = client.Request("v1/products/" + program + "/cdns");
                    content = request.ToString();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error during retrieving Ribbit cdns: " + e.Message + ", trying HTTP..");
                    try
                    {
                        using HttpResponseMessage response = await cdn.client.GetAsync(new Uri(baseUrl + program + "/" + "cdns"));
                        if (response.IsSuccessStatusCode)
                        {
                            using HttpContent res = response.Content;
                            content = await res.ReadAsStringAsync();
                        }
                        else
                        {
                            Console.WriteLine("Error during retrieving HTTP cdns: Received bad HTTP code " + response.StatusCode);
                            return cdns;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error retrieving CDNs file: " + ex.Message);
                        return cdns;
                    }
                }
            }

            var lines = content.Split(new string[] { "\n", "\r" }, StringSplitOptions.RemoveEmptyEntries);

            var lineList = new List<string>();

            for (var i = 0; i < lines.Count(); i++)
            {
                if (lines[i][0] != '#')
                {
                    lineList.Add(lines[i]);
                }
            }

            lines = lineList.ToArray();

            if (lines.Length <= 0)
            {
                return cdns;
            }
            cdns.entries = new CdnsEntry[lines.Length - 1];

            var cols = lines[0].Split('|');

            for (var c = 0; c < cols.Length; c++)
            {
                var friendlyName = cols[c].Split('!').ElementAt(0);

                for (var i = 1; i < lines.Length; i++)
                {
                    var row = lines[i].Split('|');

                    switch (friendlyName)
                    {
                        case "Name":
                            cdns.entries[i - 1].name = row[c];
                            break;
                        case "Path":
                            cdns.entries[i - 1].path = row[c];
                            break;
                        case "Hosts":
                            var hosts = row[c].Split(' ');
                            cdns.entries[i - 1].hosts = new string[hosts.Length];
                            for (var h = 0; h < hosts.Length; h++)
                            {
                                cdns.entries[i - 1].hosts[h] = hosts[h];
                            }
                            break;
                        case "ConfigPath":
                            cdns.entries[i - 1].configPath = row[c];
                            break;
                        default:
                            //Console.WriteLine("!!!!!!!! Unknown cdns variable '" + friendlyName + "'");
                            break;
                    }
                }
            }

            foreach (var subcdn in cdns.entries)
            {
                foreach (var cdnHost in subcdn.hosts)
                {
                    if (!cdn.cdnList.Contains(cdnHost))
                    {
                        cdn.cdnList.Add(cdnHost);
                    }
                }
            }

            return cdns;
        }

        private static async Task<GameBlobFile> GetProductConfig(string url, string hash)
        {
            string content;

            var gblob = new GameBlobFile();

            try
            {
                content = Encoding.UTF8.GetString(await cdn.Get(url + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash));
            }
            catch (Exception e)
            {
                Console.WriteLine("Error retrieving product config: " + e.Message);
                return gblob;
            }

            if (string.IsNullOrEmpty(content))
            {
                Console.WriteLine("Error reading product config!");
                return gblob;
            }

            dynamic json = Newtonsoft.Json.JsonConvert.DeserializeObject(content);
            if (json.all.config.decryption_key_name != null)
            {
                gblob.decryptionKeyName = json.all.config.decryption_key_name.Value;
            }
            return gblob;
        }

        private static async Task<BuildConfigFile> GetBuildConfig(string url, string hash)
        {
            string content;

            var buildConfig = new BuildConfigFile();

            try
            {
                content = Encoding.UTF8.GetString(await cdn.Get(url + "/config/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash));
            }
            catch (Exception e)
            {
                Console.WriteLine("Error retrieving build config: " + e.Message);
                return buildConfig;
            }

            if (string.IsNullOrEmpty(content) || !content.StartsWith("# Build"))
            {
                Console.WriteLine("Error reading build config!");
                return buildConfig;
            }

            var lines = content.Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

            for (var i = 0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith("#") || lines[i].Length == 0) { continue; }
                var cols = lines[i].Split(new string[] { " = " }, StringSplitOptions.RemoveEmptyEntries);
                switch (cols[0])
                {
                    case "root":
                        buildConfig.root = cols[1];
                        break;
                    case "download":
                        buildConfig.download = cols[1].Split(' ');
                        break;
                    case "install":
                        buildConfig.install = cols[1].Split(' ');
                        break;
                    case "encoding":
                        buildConfig.encoding = cols[1].Split(' ');
                        break;
                    case "encoding-size":
                        var encodingSize = cols[1].Split(' ');
                        buildConfig.encodingSize = encodingSize;
                        break;
                    case "size":
                        buildConfig.size = cols[1].Split(' ');
                        break;
                    case "size-size":
                        buildConfig.sizeSize = cols[1].Split(' ');
                        break;
                    case "build-name":
                        buildConfig.buildName = cols[1];
                        break;
                    case "build-playbuild-installer":
                        buildConfig.buildPlaybuildInstaller = cols[1];
                        break;
                    case "build-product":
                        buildConfig.buildProduct = cols[1];
                        break;
                    case "build-uid":
                        buildConfig.buildUid = cols[1];
                        break;
                    case "patch":
                        buildConfig.patch = cols[1];
                        break;
                    case "patch-size":
                        buildConfig.patchSize = cols[1];
                        break;
                    case "patch-config":
                        buildConfig.patchConfig = cols[1];
                        break;
                    case "build-branch": // Overwatch
                        buildConfig.buildBranch = cols[1];
                        break;
                    case "build-num": // Agent
                    case "build-number": // Overwatch
                    case "build-version": // Catalog
                        buildConfig.buildNumber = cols[1];
                        break;
                    case "build-attributes": // Agent
                        buildConfig.buildAttributes = cols[1];
                        break;
                    case "build-comments": // D3
                        buildConfig.buildComments = cols[1];
                        break;
                    case "build-creator": // D3
                        buildConfig.buildCreator = cols[1];
                        break;
                    case "build-fixed-hash": // S2
                        buildConfig.buildFixedHash = cols[1];
                        break;
                    case "build-replay-hash": // S2
                        buildConfig.buildReplayHash = cols[1];
                        break;
                    case "build-t1-manifest-version":
                        buildConfig.buildManifestVersion = cols[1];
                        break;
                    case "install-size":
                        buildConfig.installSize = cols[1].Split(' ');
                        break;
                    case "download-size":
                        buildConfig.downloadSize = cols[1].Split(' ');
                        break;
                    case "build-partial-priority":
                    case "partial-priority":
                        buildConfig.partialPriority = cols[1];
                        break;
                    case "partial-priority-size":
                        buildConfig.partialPrioritySize = cols[1];
                        break;
                    case "build-signature-file":
                        buildConfig.buildSignatureFile = cols[1];
                        break;
                    default:
                        Console.WriteLine("!!!!!!!! Unknown buildconfig variable '" + cols[0] + "'");
                        break;
                }
            }

            return buildConfig;
        }

        private static async Task<Dictionary<string, IndexEntry>> ParseIndex(string url, string hash, string folder = "data")
        {
            byte[] indexContent = await cdn.Get(url + folder + "/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash + ".index");

            var returnDict = new Dictionary<string, IndexEntry>();

            using (MemoryStream ms = new MemoryStream(indexContent))
            using (BinaryReader bin = new BinaryReader(ms))
            {
                bin.BaseStream.Position = bin.BaseStream.Length - 28;

                var footer = new IndexFooter
                {
                    tocHash = bin.ReadBytes(8),
                    version = bin.ReadByte(),
                    unk0 = bin.ReadByte(),
                    unk1 = bin.ReadByte(),
                    blockSizeKB = bin.ReadByte(),
                    offsetBytes = bin.ReadByte(),
                    sizeBytes = bin.ReadByte(),
                    keySizeInBytes = bin.ReadByte(),
                    checksumSize = bin.ReadByte(),
                    numElements = bin.ReadUInt32()
                };

                footer.footerChecksum = bin.ReadBytes(footer.checksumSize);

                // TODO: Read numElements as BE if it is wrong as LE
                if ((footer.numElements & 0xff000000) != 0)
                {
                    bin.BaseStream.Position -= footer.checksumSize + 4;
                    footer.numElements = bin.ReadUInt32(true);
                }

                bin.BaseStream.Position = 0;

                var indexBlockSize = 1024 * footer.blockSizeKB;
                var recordSize = footer.keySizeInBytes + footer.sizeBytes + footer.offsetBytes;
                var recordsPerBlock = indexBlockSize / recordSize;
                var recordsRead = 0;

                while (recordsRead != footer.numElements)
                {
                    var blockRecordsRead = 0;

                    for (var blockIndex = 0; blockIndex < recordsPerBlock && recordsRead < footer.numElements; blockIndex++, recordsRead++)
                    {
                        var headerHash = BitConverter.ToString(bin.ReadBytes(footer.keySizeInBytes)).Replace("-", "");
                        var entry = new IndexEntry();

                        if (footer.sizeBytes == 4)
                        {
                            entry.size = bin.ReadUInt32(true);
                        }
                        else
                        {
                            throw new NotImplementedException("Index size reading other than 4 is not implemented!");
                        }

                        if (footer.offsetBytes == 4)
                        {
                            // Archive index
                            entry.offset = bin.ReadUInt32(true);
                        }
                        else if (footer.offsetBytes == 6)
                        {
                            // Group index
                            throw new NotImplementedException("Group index reading is not implemented!");
                        }
                        else if (footer.offsetBytes == 0)
                        {
                            // File index
                        }
                        else
                        {
                            throw new NotImplementedException("Offset size reading other than 4/6/0 is not implemented!");
                        }

                        returnDict.Add(headerHash, entry);

                        blockRecordsRead++;
                    }

                    bin.ReadBytes(indexBlockSize - (blockRecordsRead * recordSize));
                }
            }

            return returnDict;
        }

        private static void GetIndexes(string url, string[] archives)
        {
            Parallel.ForEach(archives, (archive, state, i) =>
            {
                byte[] indexContent = cdn.Get(url + "/data/" + archives[i][0] + archives[i][1] + "/" + archives[i][2] + archives[i][3] + "/" + archives[i] + ".index").Result;

                using BinaryReader bin = new BinaryReader(new MemoryStream(indexContent));
                int indexEntries = indexContent.Length / 4096;

                for (var b = 0; b < indexEntries; b++)
                {
                    for (var bi = 0; bi < 170; bi++)
                    {
                        var headerHash = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");

                        var entry = new IndexEntry()
                        {
                            index = (short)i,
                            size = bin.ReadUInt32(true),
                            offset = bin.ReadUInt32(true)
                        };

                        cacheLock.EnterUpgradeableReadLock();
                        try
                        {
                            if (!indexDictionary.ContainsKey(headerHash))
                            {
                                cacheLock.EnterWriteLock();
                                try
                                {
                                    if (!indexDictionary.TryAdd(headerHash, entry))
                                    {
                                        Console.WriteLine("Duplicate index entry for " + headerHash + " " + "(index: " + archives[i] + ", size: " + entry.size + ", offset: " + entry.offset);
                                    }
                                }
                                finally
                                {
                                    cacheLock.ExitWriteLock();
                                }
                            }
                        }
                        finally
                        {
                            cacheLock.ExitUpgradeableReadLock();
                        }
                    }
                    bin.ReadBytes(16);
                }
            });
        }
        private static void GetPatchIndexes(string url, string[] archives)
        {
            Parallel.ForEach(archives, (archive, state, i) =>
            {
                byte[] indexContent = cdn.Get(url + "/patch/" + archives[i][0] + archives[i][1] + "/" + archives[i][2] + archives[i][3] + "/" + archives[i] + ".index").Result;

                using BinaryReader bin = new BinaryReader(new MemoryStream(indexContent));
                int indexEntries = indexContent.Length / 4096;

                for (var b = 0; b < indexEntries; b++)
                {
                    for (var bi = 0; bi < 170; bi++)
                    {
                        var headerHash = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");

                        var entry = new IndexEntry()
                        {
                            index = (short)i,
                            size = bin.ReadUInt32(true),
                            offset = bin.ReadUInt32(true)
                        };

                        cacheLock.EnterUpgradeableReadLock();
                        try
                        {
                            if (!patchIndexDictionary.ContainsKey(headerHash))
                            {
                                cacheLock.EnterWriteLock();
                                try
                                {
                                    if (!patchIndexDictionary.TryAdd(headerHash, entry))
                                    {
                                        Console.WriteLine("Duplicate patch index entry for " + headerHash + " " + "(index: " + archives[i] + ", size: " + entry.size + ", offset: " + entry.offset);
                                    }
                                }
                                finally
                                {
                                    cacheLock.ExitWriteLock();
                                }
                            }
                        }
                        finally
                        {
                            cacheLock.ExitUpgradeableReadLock();
                        }
                    }
                    bin.ReadBytes(16);
                }
            });
        }

        private static async Task<RootFile> GetRoot(string url, string hash, bool parseIt = false)
        {
            var root = new RootFile
            {
                entriesLookup = new MultiDictionary<ulong, RootEntry>(),
                entriesFDID = new MultiDictionary<uint, RootEntry>()
            };

            byte[] content = await cdn.Get(url + "/data/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash);
            if (!parseIt) return root;

            var namedCount = 0;
            var unnamedCount = 0;
            uint totalFiles = 0;
            uint namedFiles = 0;
            var newRoot = false;
            using (BinaryReader bin = new BinaryReader(new MemoryStream(BLTE.Parse(content))))
            {
                var header = bin.ReadUInt32();

                if (header == 0x4D465354)
                {
                    totalFiles = bin.ReadUInt32();
                    namedFiles = bin.ReadUInt32();
                    newRoot = true;
                }
                else
                {
                    bin.BaseStream.Position = 0;
                }

                var blockCount = 0;

                while (bin.BaseStream.Position < bin.BaseStream.Length)
                {
                    var count = bin.ReadUInt32();
                    var contentFlags = (ContentFlags)bin.ReadUInt32();
                    var localeFlags = (LocaleFlags)bin.ReadUInt32();

                    //Console.WriteLine("[Block " + blockCount + "] " + count + " entries. Content flags: " + contentFlags.ToString() + ", Locale flags: " + localeFlags.ToString());
                    var entries = new RootEntry[count];
                    var filedataIds = new int[count];

                    var fileDataIndex = 0;
                    for (var i = 0; i < count; ++i)
                    {
                        entries[i].localeFlags = localeFlags;
                        entries[i].contentFlags = contentFlags;

                        filedataIds[i] = fileDataIndex + bin.ReadInt32();
                        entries[i].fileDataID = (uint)filedataIds[i];
                        fileDataIndex = filedataIds[i] + 1;
                    }

                    var blockFdids = new List<string>();
                    if (!newRoot)
                    {
                        for (var i = 0; i < count; ++i)
                        {
                            entries[i].md5 = bin.ReadBytes(16);
                            entries[i].lookup = bin.ReadUInt64();
                            root.entriesLookup.Add(entries[i].lookup, entries[i]);
                            root.entriesFDID.Add(entries[i].fileDataID, entries[i]);
                            blockFdids.Add(entries[i].fileDataID.ToString());
                        }
                    }
                    else
                    {
                        for (var i = 0; i < count; ++i)
                        {
                            entries[i].md5 = bin.ReadBytes(16);
                        }

                        for (var i = 0; i < count; ++i)
                        {
                            if (contentFlags.HasFlag(ContentFlags.NoNames))
                            {
                                entries[i].lookup = 0;
                                unnamedCount++;
                            }
                            else
                            {
                                entries[i].lookup = bin.ReadUInt64();
                                root.entriesLookup.Add(entries[i].lookup, entries[i]);
                                namedCount++;
                            }

                            root.entriesFDID.Add(entries[i].fileDataID, entries[i]);
                            blockFdids.Add(entries[i].fileDataID.ToString());
                        }
                    }

                    //File.WriteAllLinesAsync("blocks/Block" + blockCount + ".txt", blockFdids);
                    blockCount++;
                }
            }

            if ((namedFiles > 0) && namedFiles != namedCount)
                throw new Exception("Didn't read correct amount of named files! Read " + namedCount + " but expected " + namedFiles);

            if ((totalFiles > 0) && totalFiles != (namedCount + unnamedCount))
                throw new Exception("Didn't read correct amount of total files! Read " + (namedCount + unnamedCount) + " but expected " + totalFiles);

            return root;
        }

        private static async Task<DownloadFile> GetDownload(string url, string hash, bool parseIt = false)
        {
            var download = new DownloadFile();

            byte[] content = await cdn.Get(url + "/data/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash);

            if (!parseIt) return download;

            using (BinaryReader bin = new BinaryReader(new MemoryStream(BLTE.Parse(content))))
            {
                if (Encoding.UTF8.GetString(bin.ReadBytes(2)) != "DL") { throw new Exception("Error while parsing download file. Did BLTE header size change?"); }
                download.unk = bin.ReadBytes(3); // Unk
                download.numEntries = bin.ReadUInt32(true);
                download.numTags = bin.ReadUInt16(true);

                download.entries = new DownloadEntry[download.numEntries];
                for (int i = 0; i < download.numEntries; i++)
                {
                    download.entries[i].hash = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");
                    bin.ReadBytes(10);
                }
            }

            return download;
        }

        private static async Task<InstallFile> GetInstall(string url, string hash, bool parseIt = false)
        {
            var install = new InstallFile();

            byte[] content = await cdn.Get(url + "/data/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash);

            if (!parseIt) return install;

            using (BinaryReader bin = new BinaryReader(new MemoryStream(BLTE.Parse(content))))
            {
                if (Encoding.UTF8.GetString(bin.ReadBytes(2)) != "IN") { throw new Exception("Error while parsing install file. Did BLTE header size change?"); }

                bin.ReadByte();

                install.hashSize = bin.ReadByte();
                if (install.hashSize != 16) throw new Exception("Unsupported install hash size!");

                install.numTags = bin.ReadUInt16(true);
                install.numEntries = bin.ReadUInt32(true);

                int bytesPerTag = ((int)install.numEntries + 7) / 8;

                install.tags = new InstallTagEntry[install.numTags];

                for (var i = 0; i < install.numTags; i++)
                {
                    install.tags[i].name = bin.ReadCString();
                    install.tags[i].type = bin.ReadUInt16(true);

                    var filebits = bin.ReadBytes(bytesPerTag);

                    for (int j = 0; j < bytesPerTag; j++)
                        filebits[j] = (byte)((filebits[j] * 0x0202020202 & 0x010884422010) % 1023);

                    install.tags[i].files = new BitArray(filebits);
                }

                install.entries = new InstallFileEntry[install.numEntries];

                for (var i = 0; i < install.numEntries; i++)
                {
                    install.entries[i].name = bin.ReadCString();
                    install.entries[i].contentHash = bin.ReadBytes(install.hashSize);
                    install.entries[i].size = bin.ReadUInt32(true);
                    install.entries[i].tags = new List<string>();
                    for (var j = 0; j < install.numTags; j++)
                    {
                        if (install.tags[j].files[i] == true)
                        {
                            install.entries[i].tags.Add(install.tags[j].type + "=" + install.tags[j].name);
                        }
                    }
                }
            }

            return install;
        }

        private static async Task<EncodingFile> GetEncoding(string url, string hash, int encodingSize = 0, bool parseTableB = false, bool checkStuff = false)
        {
            var encoding = new EncodingFile();

            byte[] content;

            content = await cdn.Get(url + "/data/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash);

            if (encodingSize != 0 && encodingSize != content.Length)
            {
                content = await cdn.Get(url + "/data/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash, true);

                if (encodingSize != content.Length && encodingSize != 0)
                {
                    throw new Exception("File corrupt/not fully downloaded! Remove " + "data / " + hash[0] + hash[1] + " / " + hash[2] + hash[3] + " / " + hash + " from cache.");
                }
            }

            using (BinaryReader bin = new BinaryReader(new MemoryStream(BLTE.Parse(content))))
            {
                if (Encoding.UTF8.GetString(bin.ReadBytes(2)) != "EN") { throw new Exception("Error while parsing encoding file. Did BLTE header size change?"); }
                encoding.unk1 = bin.ReadByte();
                encoding.checksumSizeA = bin.ReadByte();
                encoding.checksumSizeB = bin.ReadByte();
                encoding.sizeA = bin.ReadUInt16(true);
                encoding.sizeB = bin.ReadUInt16(true);
                encoding.numEntriesA = bin.ReadUInt32(true);
                encoding.numEntriesB = bin.ReadUInt32(true);
                bin.ReadByte(); // unk
                encoding.stringBlockSize = bin.ReadUInt32(true);

                var headerLength = bin.BaseStream.Position;
                var stringBlockEntries = new List<string>();

                if (parseTableB)
                {
                    while ((bin.BaseStream.Position - headerLength) != (long)encoding.stringBlockSize)
                    {
                        stringBlockEntries.Add(bin.ReadCString());
                    }

                    encoding.stringBlockEntries = stringBlockEntries.ToArray();
                }
                else
                {
                    bin.BaseStream.Position += (long)encoding.stringBlockSize;
                }

                /* Table A */
                if (checkStuff)
                {
                    encoding.aHeaders = new EncodingHeaderEntry[encoding.numEntriesA];

                    for (int i = 0; i < encoding.numEntriesA; i++)
                    {
                        encoding.aHeaders[i].firstHash = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");
                        encoding.aHeaders[i].checksum = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");
                    }
                }
                else
                {
                    bin.BaseStream.Position += encoding.numEntriesA * 32;
                }

                var tableAstart = bin.BaseStream.Position;

                List<EncodingFileEntry> entries = new List<EncodingFileEntry>();

                for (int i = 0; i < encoding.numEntriesA; i++)
                {
                    ushort keysCount;
                    while ((keysCount = bin.ReadUInt16()) != 0)
                    {
                        EncodingFileEntry entry = new EncodingFileEntry()
                        {
                            keyCount = keysCount,
                            size = bin.ReadUInt32(true),
                            cKey = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", ""),
                            eKeys = new List<string>()
                        };

                        for (int key = 0; key < entry.keyCount; key++)
                        {
                            entry.eKeys.Add(BitConverter.ToString(bin.ReadBytes(16)).Replace("-", ""));
                        }

                        entries.Add(entry);
                    }

                    var remaining = 4096 - ((bin.BaseStream.Position - tableAstart) % 4096);
                    if (remaining > 0) { bin.BaseStream.Position += remaining; }
                }

                encoding.aEntries = entries.ToArray();

                if (!parseTableB)
                {
                    return encoding;
                }

                /* Table B */
                if (checkStuff)
                {
                    encoding.bHeaders = new EncodingHeaderEntry[encoding.numEntriesB];

                    for (int i = 0; i < encoding.numEntriesB; i++)
                    {
                        encoding.bHeaders[i].firstHash = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");
                        encoding.bHeaders[i].checksum = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");
                    }
                }
                else
                {
                    bin.BaseStream.Position += encoding.numEntriesB * 32;
                }

                var tableBstart = bin.BaseStream.Position;

                encoding.bEntries = new Dictionary<string, EncodingFileDescEntry>();

                while (bin.BaseStream.Position < tableBstart + 4096 * encoding.numEntriesB)
                {
                    var remaining = 4096 - (bin.BaseStream.Position - tableBstart) % 4096;

                    if (remaining < 25)
                    {
                        bin.BaseStream.Position += remaining;
                        continue;
                    }

                    var key = BitConverter.ToString(bin.ReadBytes(16)).Replace("-", "");

                    EncodingFileDescEntry entry = new EncodingFileDescEntry()
                    {
                        stringIndex = bin.ReadUInt32(true),
                        compressedSize = bin.ReadUInt40(true)
                    };

                    if (entry.stringIndex == uint.MaxValue) break;

                    encoding.bEntries.Add(key, entry);
                }

                // Go to the end until we hit a non-NUL byte
                while (bin.BaseStream.Position < bin.BaseStream.Length)
                {
                    if (bin.ReadByte() != 0)
                        break;
                }

                bin.BaseStream.Position -= 1;
                var eespecSize = bin.BaseStream.Length - bin.BaseStream.Position;
                encoding.encodingESpec = new string(bin.ReadChars(int.Parse(eespecSize.ToString())));
            }

            return encoding;
        }

        private static async Task<PatchFile> GetPatch(string url, string hash, bool parseIt = false)
        {
            var patchFile = new PatchFile();

            byte[] content = await cdn.Get(url + "/patch/" + hash[0] + hash[1] + "/" + hash[2] + hash[3] + "/" + hash);

            if (!parseIt) return patchFile;

            using (BinaryReader bin = new BinaryReader(new MemoryStream(content)))
            {
                if (Encoding.UTF8.GetString(bin.ReadBytes(2)) != "PA") { throw new Exception("Error while parsing patch file!"); }

                patchFile.version = bin.ReadByte();
                patchFile.fileKeySize = bin.ReadByte();
                patchFile.sizeB = bin.ReadByte();
                patchFile.patchKeySize = bin.ReadByte();
                patchFile.blockSizeBits = bin.ReadByte();
                patchFile.blockCount = bin.ReadUInt16(true);
                patchFile.flags = bin.ReadByte();
                patchFile.encodingContentKey = bin.ReadBytes(16);
                patchFile.encodingEncodingKey = bin.ReadBytes(16);
                patchFile.decodedSize = bin.ReadUInt32(true);
                patchFile.encodedSize = bin.ReadUInt32(true);
                patchFile.especLength = bin.ReadByte();
                patchFile.encodingSpec = new string(bin.ReadChars(patchFile.especLength));

                patchFile.blocks = new PatchBlock[patchFile.blockCount];
                for (var i = 0; i < patchFile.blockCount; i++)
                {
                    patchFile.blocks[i].lastFileContentKey = bin.ReadBytes(patchFile.fileKeySize);
                    patchFile.blocks[i].blockMD5 = bin.ReadBytes(16);
                    patchFile.blocks[i].blockOffset = bin.ReadUInt32(true);

                    var prevPos = bin.BaseStream.Position;

                    var files = new List<BlockFile>();

                    bin.BaseStream.Position = patchFile.blocks[i].blockOffset;
                    while (bin.BaseStream.Position <= patchFile.blocks[i].blockOffset + 0x10000)
                    {
                        var file = new BlockFile();

                        file.numPatches = bin.ReadByte();
                        if (file.numPatches == 0) break;
                        file.targetFileContentKey = bin.ReadBytes(patchFile.fileKeySize);
                        file.decodedSize = bin.ReadUInt40(true);

                        var filePatches = new List<FilePatch>();

                        for (var j = 0; j < file.numPatches; j++)
                        {
                            var filePatch = new FilePatch();
                            filePatch.sourceFileEncodingKey = bin.ReadBytes(patchFile.fileKeySize);
                            filePatch.decodedSize = bin.ReadUInt40(true);
                            filePatch.patchEncodingKey = bin.ReadBytes(patchFile.patchKeySize);
                            filePatch.patchSize = bin.ReadUInt32(true);
                            filePatch.patchIndex = bin.ReadByte();
                            filePatches.Add(filePatch);
                        }

                        file.patches = filePatches.ToArray();

                        files.Add(file);
                    }

                    patchFile.blocks[i].files = files.ToArray();
                    bin.BaseStream.Position = prevPos;
                }
            }

            return patchFile;
        }
    }
}

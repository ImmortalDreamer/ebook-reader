/**
 * @license BSD-3-Clause
 * Copyright (c) 2026, ッツ Reader Authors
 * All rights reserved.
 */

import type { BookCardProps } from '$lib/components/book-card/book-card-props';
import type {
  BooksDbAudioBook,
  BooksDbBookData,
  BooksDbBookmarkData,
  BooksDbReadingGoal,
  BooksDbStatistic,
  BooksDbSubtitleData
} from '$lib/data/database/books-db/versions/books-db';
import { logger } from '$lib/data/logger';
import { MergeMode } from '$lib/data/merge-mode';
import { mergeReadingGoals, readingGoalSortFunction } from '$lib/data/reading-goal';
import {
  BaseStorageHandler,
  FilePrefix,
  type ExternalFile
} from '$lib/data/storage/handler/base-handler';
import { StorageKey } from '$lib/data/storage/storage-types';
import { database } from '$lib/data/store';
import {
  handleErrorDuringReplication
} from '$lib/functions/replication/error-handler';
import { AbortError, throwIfAborted } from '$lib/functions/replication/replication-error';
import { ReplicationSaveBehavior } from '$lib/functions/replication/replication-options';
import { replicationProgress$ } from '$lib/functions/replication/replication-progress';
import { mergeStatistics, updateStatisticToStore } from '$lib/functions/statistic-util';
import pLimit from 'p-limit';
import { storageRootName } from '$lib/data/env';
import { unlockStorageData } from '$lib/data/storage/storage-source-manager';

interface WebDavFile {
  href: string;
  displayName: string;
  contentLength: number;
  lastModified: string;
  isDirectory: boolean;
}

interface WebDavContext {
  serverUrl: string;
  username: string;
  password: string;
}

export class WebDavStorageHandler extends BaseStorageHandler {
  private serverUrl = '';
  private username = '';
  private password = '';
  private basePath = '';
  private titleToFiles = new Map<string, ExternalFile[]>();
  private rootFiles = new Map<string, ExternalFile>();
  private rootFileListFetched = false;
  private webdavContext: WebDavContext | undefined;

  constructor(window: Window) {
    super(window, StorageKey.WEBDAV);
  }

  updateSettings(
    window: Window,
    isForBrowser: boolean,
    saveBehavior: ReplicationSaveBehavior,
    statisticsMergeMode: MergeMode,
    readingGoalsMergeMode: MergeMode,
    cacheStorageData: boolean,
    askForStorageUnlock: boolean,
    storageSourceName: string
  ) {
    this.window = window;
    this.isForBrowser = isForBrowser;
    this.saveBehavior = saveBehavior;
    this.cacheStorageData = cacheStorageData;
    this.askForStorageUnlock = askForStorageUnlock;
    this.statisticsMergeMode = statisticsMergeMode;
    this.readingGoalsMergeMode = readingGoalsMergeMode;
    
    const newStorageSource = storageSourceName;
    if (newStorageSource !== this.storageSourceName) {
      this.clearData();
      this.webdavContext = undefined;
    }
    
    this.storageSourceName = newStorageSource;
  }

  private async ensureWebDavContext(): Promise<void> {
    if (this.webdavContext) {
      this.serverUrl = this.webdavContext.serverUrl.replace(/\/$/, '');
      this.username = this.webdavContext.username;
      this.password = this.webdavContext.password;
      this.basePath = `${this.serverUrl}/${storageRootName}`;
      return;
    }

    const db = await database.db;
    const storageSource = await db.get('storageSource', this.storageSourceName);

    if (!storageSource) {
      throw new Error(`No storage source with name ${this.storageSourceName} found`);
    }

    const unlockResult = await unlockStorageData(
      storageSource,
      'You are trying to access WebDAV storage',
      this.askForStorageUnlock
        ? {
            action: `Enter the credentials for ${this.storageSourceName} to proceed`,
            encryptedData: storageSource.data,
            forwardSecret: true
          }
        : undefined
    );

    if (!unlockResult || !unlockResult.serverUrl || !unlockResult.username || !unlockResult.password) {
      throw new Error('Unable to unlock WebDAV credentials');
    }

    this.webdavContext = {
      serverUrl: unlockResult.serverUrl,
      username: unlockResult.username,
      password: unlockResult.password
    };

    this.serverUrl = this.webdavContext.serverUrl.replace(/\/$/, '');
    this.username = this.webdavContext.username;
    this.password = this.webdavContext.password;
    this.basePath = `${this.serverUrl}/${storageRootName}`;
  }

  clearData(clearAll = true) {
    this.titleToFiles.clear();
    this.rootFiles.clear();
    this.rootFileListFetched = false;

    if (clearAll) {
      this.titleToBookCard.clear();
      this.dataListFetched = false;
    }
  }

  private getAuthHeader(): string {
    const credentials = btoa(`${this.username}:${this.password}`);
    return `Basic ${credentials}`;
  }

  private async request(
    url: string,
    options: {
      method?: string;
      headers?: Record<string, string>;
      body?: XMLHttpRequestBodyInit | null;
      responseType?: XMLHttpRequestResponseType;
      trackProgress?: boolean;
      progressBase?: number;
    } = {}
  ): Promise<any> {
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open(options.method || 'GET', url);

      xhr.setRequestHeader('Authorization', this.getAuthHeader());
      
      if (options.headers) {
        Object.entries(options.headers).forEach(([key, value]) => {
          xhr.setRequestHeader(key, value);
        });
      }

      if (options.responseType) {
        xhr.responseType = options.responseType;
      }

      if (options.trackProgress) {
        xhr.onprogress = (event) => {
          if (event.lengthComputable) {
            const progressToAdd = (event.loaded / event.total) * 100;
            replicationProgress$.next({
              progressBase: options.progressBase || 0,
              maxProgress: 100,
              progressToAdd,
              completeStep: false
            });
          }
        };
      }

      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(xhr.response);
        } else {
          reject(new Error(`WebDAV request failed: ${xhr.status} ${xhr.statusText}`));
        }
      };

      xhr.onerror = () => reject(new Error('WebDAV request failed'));
      xhr.onabort = () => reject(new AbortError());

      xhr.send(options.body);
    });
  }

  private parseWebDavResponse(xmlText: string, baseUrl: string): WebDavFile[] {
    const parser = new DOMParser();
    const xmlDoc = parser.parseFromString(xmlText, 'text/xml');
    const responses = xmlDoc.getElementsByTagNameNS('DAV:', 'response');
    const files: WebDavFile[] = [];

    for (let i = 0; i < responses.length; i++) {
      const response = responses[i];
      const hrefElement = response.getElementsByTagNameNS('DAV:', 'href')[0];
      const propstat = response.getElementsByTagNameNS('DAV:', 'propstat')[0];
      
      if (!hrefElement || !propstat) continue;

      const href = hrefElement.textContent || '';
      const prop = propstat.getElementsByTagNameNS('DAV:', 'prop')[0];
      
      if (!prop) continue;

      const displayNameElement = prop.getElementsByTagNameNS('DAV:', 'displayname')[0];
      const contentLengthElement = prop.getElementsByTagNameNS('DAV:', 'getcontentlength')[0];
      const lastModifiedElement = prop.getElementsByTagNameNS('DAV:', 'getlastmodified')[0];
      const resourceTypeElement = prop.getElementsByTagNameNS('DAV:', 'resourcetype')[0];
      
      const isDirectory = resourceTypeElement?.getElementsByTagNameNS('DAV:', 'collection').length > 0;
      
      // Skip the base directory itself
      if (href === baseUrl || href === baseUrl + '/') continue;

      files.push({
        href: decodeURIComponent(href),
        displayName: displayNameElement?.textContent || href.split('/').filter(Boolean).pop() || '',
        contentLength: parseInt(contentLengthElement?.textContent || '0', 10),
        lastModified: lastModifiedElement?.textContent || '',
        isDirectory
      });
    }

    return files;
  }

  private async listDirectory(path: string): Promise<WebDavFile[]> {
    const propfindBody = `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:displayname/>
    <D:getcontentlength/>
    <D:getlastmodified/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>`;

    const response = await this.request(path, {
      method: 'PROPFIND',
      headers: {
        'Content-Type': 'application/xml',
        'Depth': '1'
      },
      body: propfindBody,
      responseType: 'text'
    });

    return this.parseWebDavResponse(response, path);
  }

  private async ensureDirectory(path: string): Promise<void> {
    try {
      await this.request(path, { method: 'PROPFIND', headers: { 'Depth': '0' } });
    } catch (error) {
      // Directory doesn't exist, create it
      await this.request(path, { method: 'MKCOL' });
    }
  }

  private async ensureBookDirectory(title: string): Promise<string> {
    const bookPath = `${this.basePath}/${encodeURIComponent(title)}`;
    await this.ensureDirectory(bookPath);
    return bookPath;
  }

  private async getFilesForTitle(title: string): Promise<ExternalFile[]> {
    if (this.cacheStorageData && this.titleToFiles.has(title)) {
      return this.titleToFiles.get(title)!;
    }

    const bookPath = `${this.basePath}/${encodeURIComponent(title)}`;
    
    try {
      const files = await this.listDirectory(bookPath);
      const externalFiles = files
        .filter(f => !f.isDirectory)
        .map(f => ({
          id: f.href,
          name: f.displayName
        }));

      if (this.cacheStorageData) {
        this.titleToFiles.set(title, externalFiles);
      }

      return externalFiles;
    } catch (error) {
      // Directory doesn't exist yet
      return [];
    }
  }

  private async getRootFiles(): Promise<void> {
    if (this.rootFileListFetched) return;

    await this.ensureDirectory(this.basePath);
    const files = await this.listDirectory(this.basePath);
    
    this.rootFiles.clear();
    files
      .filter(f => !f.isDirectory)
      .forEach(f => {
        this.rootFiles.set(f.displayName, { id: f.href, name: f.displayName });
      });

    this.rootFileListFetched = true;
  }

  async getBookList(cancelSignal?: AbortSignal): Promise<BookCardProps[]> {
    throwIfAborted(cancelSignal);

    if (this.dataListFetched) {
      return Array.from(this.titleToBookCard.values());
    }

    await this.ensureWebDavContext();
    await this.ensureDirectory(this.basePath);
    const files = await this.listDirectory(this.basePath);
    const directories = files.filter(f => f.isDirectory);

    const bookCards: BookCardProps[] = [];

    for (const dir of directories) {
      throwIfAborted(cancelSignal);

      const title = dir.displayName;
      const bookFiles = await this.getFilesForTitle(title);
      const bookDataFile = bookFiles.find(f => f.name.startsWith('bookdata_'));

      if (bookDataFile) {
        const metadata = this.extractMetadata(bookDataFile.name);
        const bookCard: BookCardProps = {
          title,
          imagePath: '',
          characters: metadata.characters,
          lastBookModified: metadata.lastBookModified,
          lastBookOpen: metadata.lastBookOpen,
          storageSource: this.storageSourceName,
          isPlaceholder: false
        };

        this.titleToBookCard.set(title, bookCard);
        bookCards.push(bookCard);
      }
    }

    this.dataListFetched = true;
    return bookCards;
  }

  async getBook(
    title: string,
    lastBookModified: number,
    lastBookOpen: number,
    cancelSignal?: AbortSignal
  ): Promise<Omit<BooksDbBookData, 'id'> | File | undefined> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const files = await this.getFilesForTitle(title);
    const bookDataFile = files.find(f => f.name.startsWith('bookdata_'));

    if (!bookDataFile) return undefined;

    const blob = await this.request(bookDataFile.id, {
      responseType: 'blob',
      trackProgress: true,
      progressBase: 0
    });

    return new File([blob], bookDataFile.name);
  }

  async getProgress(
    title: string,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<BooksDbBookmarkData | File | undefined> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const files = await this.getFilesForTitle(title);
    const progressFile = files.find(f => f.name.startsWith('progress_'));

    if (!progressFile) return undefined;

    const text = await this.request(progressFile.id, {
      responseType: 'text'
    });

    return JSON.parse(text);
  }

  async getStatistics(
    title: string,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<{ statistics: BooksDbStatistic[]; lastStatisticModified: number }> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const files = await this.getFilesForTitle(title);
    const statsFile = files.find(f => f.name.startsWith('statistics_'));

    if (!statsFile) {
      return { statistics: [], lastStatisticModified: 0 };
    }

    const text = await this.request(statsFile.id, {
      responseType: 'text'
    });

    const data = JSON.parse(text);
    return {
      statistics: data.statistics || [],
      lastStatisticModified: this.extractMetadata(statsFile.name).lastStatisticModified
    };
  }

  async saveBook(
    title: string,
    data: Omit<BooksDbBookData, 'id'> | File,
    lastBookModified: number,
    lastBookOpen: number,
    cancelSignal?: AbortSignal
  ): Promise<number> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const bookPath = await this.ensureBookDirectory(title);
    const files = await this.getFilesForTitle(title);

    let blob: Blob;
    let filename: string;

    if (data instanceof File) {
      blob = data;
      filename = data.name;
    } else {
      const zipBlob = await this.createBookZip(data);
      filename = this.createBookDataName(
        data.lastBookModified,
        data.lastBookOpen,
        data.characters
      );
      blob = zipBlob;
    }

    // Delete old book data files
    const oldFiles = files.filter(f => f.name.startsWith('bookdata_'));
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${bookPath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/zip'
      },
      body: blob
    });

    // Invalidate cache
    this.titleToFiles.delete(title);

    return blob.size;
  }

  async saveProgress(
    title: string,
    data: BooksDbBookmarkData | File,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<void> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const bookPath = await this.ensureBookDirectory(title);
    const files = await this.getFilesForTitle(title);

    let content: string;
    let filename: string;

    if (data instanceof File) {
      content = await data.text();
      filename = data.name;
    } else {
      content = JSON.stringify(data);
      filename = this.createProgressName(lastBookModified, data.lastBookmarkModified);
    }

    // Delete old progress files
    const oldFiles = files.filter(f => f.name.startsWith('progress_'));
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${bookPath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: content
    });

    // Invalidate cache
    this.titleToFiles.delete(title);
  }

  async saveStatistics(
    title: string,
    statistics: BooksDbStatistic[],
    lastBookModified: number,
    lastStatisticModified: number,
    cancelSignal?: AbortSignal
  ): Promise<void> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const bookPath = await this.ensureBookDirectory(title);
    const files = await this.getFilesForTitle(title);

    const content = JSON.stringify({ statistics });
    const filename = this.createStatisticsName(lastBookModified, lastStatisticModified);

    // Delete old statistics files
    const oldFiles = files.filter(f => f.name.startsWith('statistics_'));
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${bookPath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: content
    });

    // Invalidate cache
    this.titleToFiles.delete(title);
  }

  async deleteBookData(
    booksToDelete: string[],
    cancelSignal: AbortSignal
  ): Promise<{ deletedTitles: string[]; undeletedTitles: string[] }> {
    await this.ensureWebDavContext();
    const deletedTitles: string[] = [];
    const undeletedTitles: string[] = [];

    for (const title of booksToDelete) {
      throwIfAborted(cancelSignal);

      try {
        const bookPath = `${this.basePath}/${encodeURIComponent(title)}`;
        
        // Delete all files in the directory first
        const files = await this.getFilesForTitle(title);
        for (const file of files) {
          await this.request(file.id, { method: 'DELETE' });
        }

        // Delete the directory
        await this.request(bookPath, { method: 'DELETE' });

        deletedTitles.push(title);
        this.titleToBookCard.delete(title);
        this.titleToFiles.delete(title);
      } catch (error) {
        logger.error(`Failed to delete book ${title}:`, error);
        undeletedTitles.push(title);
      }
    }

    return { deletedTitles, undeletedTitles };
  }

  async getReadingGoal(cancelSignal?: AbortSignal): Promise<BooksDbReadingGoal[]> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    await this.getRootFiles();
    const goalFile = Array.from(this.rootFiles.values()).find(f =>
      f.name.startsWith('ttu-user-goals_')
    );

    if (!goalFile) return [];

    const text = await this.request(goalFile.id, { responseType: 'text' });
    const data = JSON.parse(text);
    return data.readingGoals || [];
  }

  async saveReadingGoal(
    readingGoals: BooksDbReadingGoal[],
    lastGoalModified: number,
    cancelSignal?: AbortSignal
  ): Promise<void> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    await this.getRootFiles();

    const content = JSON.stringify({ readingGoals });
    const filename = `ttu-user-goals_${lastGoalModified}.json`;

    // Delete old goal files
    const oldFiles = Array.from(this.rootFiles.values()).filter(f =>
      f.name.startsWith('ttu-user-goals_')
    );
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${this.basePath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: content
    });

    this.rootFileListFetched = false;
  }

  async getAudioBook(
    title: string,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<BooksDbAudioBook | undefined> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const files = await this.getFilesForTitle(title);
    const audioFile = files.find(f => f.name.startsWith(FilePrefix.AUDIO_BOOK));

    if (!audioFile) return undefined;

    const text = await this.request(audioFile.id, { responseType: 'text' });
    return JSON.parse(text);
  }

  async saveAudioBook(
    title: string,
    audioBook: BooksDbAudioBook,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<void> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const bookPath = await this.ensureBookDirectory(title);
    const files = await this.getFilesForTitle(title);

    const content = JSON.stringify(audioBook);
    const filename = `${FilePrefix.AUDIO_BOOK}${lastBookModified}.json`;

    // Delete old audio files
    const oldFiles = files.filter(f => f.name.startsWith(FilePrefix.AUDIO_BOOK));
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${bookPath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: content
    });

    this.titleToFiles.delete(title);
  }

  async getSubtitle(
    title: string,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<BooksDbSubtitleData | undefined> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const files = await this.getFilesForTitle(title);
    const subtitleFile = files.find(f => f.name.startsWith(FilePrefix.SUBTITLE));

    if (!subtitleFile) return undefined;

    const text = await this.request(subtitleFile.id, { responseType: 'text' });
    return JSON.parse(text);
  }

  async saveSubtitle(
    title: string,
    subtitle: BooksDbSubtitleData,
    lastBookModified: number,
    cancelSignal?: AbortSignal
  ): Promise<void> {
    throwIfAborted(cancelSignal);
    await this.ensureWebDavContext();

    const bookPath = await this.ensureBookDirectory(title);
    const files = await this.getFilesForTitle(title);

    const content = JSON.stringify(subtitle);
    const filename = `${FilePrefix.SUBTITLE}${lastBookModified}.json`;

    // Delete old subtitle files
    const oldFiles = files.filter(f => f.name.startsWith(FilePrefix.SUBTITLE));
    for (const oldFile of oldFiles) {
      await this.request(oldFile.id, { method: 'DELETE' });
    }

    // Upload new file
    const uploadUrl = `${bookPath}/${encodeURIComponent(filename)}`;
    await this.request(uploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      body: content
    });

    this.titleToFiles.delete(title);
  }

  async prepareBookForReading(
    title: string,
    lastBookModified: number,
    lastBookOpen: number,
    cancelSignal?: AbortSignal
  ): Promise<number> {
    await this.ensureWebDavContext();
    const data = await database.getDataByTitle(title);

    let idToReturn = 0;
    let bookData: Omit<BooksDbBookData, 'id'> | undefined = data;

    if (!bookData || !bookData.elementHtml) {
      const file = await this.getBook(title, lastBookModified, lastBookOpen, cancelSignal);

      bookData = file
        ? bookData || {
            title,
            styleSheet: '',
            elementHtml: '',
            blobs: {},
            coverImage: '',
            hasThumb: true,
            characters: 0,
            sections: [],
            lastBookModified: 0,
            lastBookOpen: 0,
            storageSource: undefined
          }
        : undefined;
    }

    if (!bookData) {
      throw new Error('No local or external book data found');
    }

    if (bookData.elementHtml) {
      idToReturn = await database.addData(bookData);
    }

    return idToReturn;
  }
}
